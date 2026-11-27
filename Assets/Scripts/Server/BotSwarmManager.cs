using System.Collections.Generic;
using System.IO;
using System.Text;
using Unity.Collections;
using Unity.Networking.Transport;
using Unity.Networking.Transport.Utilities;
using UnityEngine;


public class BotSwarmManager : MonoBehaviour
{
    [Header("Server Endpoints")]
    public string serverIp = "127.0.0.1";
    public ushort region0Port = 7777;
    public ushort region1Port = 7778;

    [Header("Swarm Scale & Ramp-Up")]
    public int targetBotCount = 1000;
    public int pooledDriverCount = 10;      // 10 drivers x 100 connections = 1,000 independent sockets
    public float spawnRatePerSecond = 100f; // Staggered ramp-up prevents Frame-1 handshake flood

    [Header("Map Boundaries")]
    public float maxX = 50f;
    public float maxZ = 50f;

    [Header("Optional GPU Visualization")]
    public bool enableVisualization = true;
    public int maxVisualizedBots = 1000;
    public Mesh botMesh;                    // Assign Unity's default Capsule mesh in Inspector
    public Material botMaterial;            // Assign a standard material (with GPU Instancing checked)

    private class VirtualBot
    {
        public int playerId;
        public int driverIndex;

        public NetworkConnection serverConn;
        public NetworkConnection pendingConn;
        public NetworkConnection retiringConn;
        public float retiringConnTime;

        public uint inputSeq;
        public Vector2 currentPosition;
        public Vector2 moveDir;
        public float nextDirectionChangeTime;

        public bool connectedToActiveServer;
        public int activeRegionIndex;
        public bool handoffInProgress;
        public float handoffStartTime;
    }

    private NetworkDriver[] drivers;
    private NetworkPipeline[] simPipelines;
    private List<VirtualBot> bots = new List<VirtualBot>();
    private Dictionary<int, VirtualBot> botLookup = new Dictionary<int, VirtualBot>();

    private float spawnAccumulator = 0f;
    private int nextBotId = 10000;

    // Aggregated CSV logging (sampled to keep memory/IO minimal at 1,000+ bots)
    private StringBuilder csvLog = new StringBuilder();
    private string logPath;
    private float nextLogTime = 1f;
    private List<double> rttWindowSamples = new List<double>(4096);
    private List<double> handoffWindowSamples = new List<double>(512);

    // Pre-allocated matrix buffer for GPU instanced rendering (max 1023 per batch)
    private Matrix4x4[] renderMatrices = new Matrix4x4[1023];

    void Start()
    {
        Application.targetFrameRate = 60;

        // Parse optional command-line overrides for automated headless sweeps
        string[] args = System.Environment.GetCommandLineArgs();
        for (int i = 0; i < args.Length - 1; i++)
        {
            if (args[i] == "-bots" && int.TryParse(args[i + 1], out int b)) targetBotCount = b;
            if (args[i] == "-drivers" && int.TryParse(args[i + 1], out int d)) pooledDriverCount = d;
            if (args[i] == "-visual" && int.TryParse(args[i + 1], out int v)) enableVisualization = (v == 1);
        }

        // Create pooled NetworkDrivers with matching SimulatorPipelineStage layout
        drivers = new NetworkDriver[pooledDriverCount];
        simPipelines = new NetworkPipeline[pooledDriverCount];

        for (int i = 0; i < pooledDriverCount; i++)
        {
            var settings = new NetworkSettings();
            settings.WithNetworkConfigParameters(
                sendQueueCapacity: 2048,
                receiveQueueCapacity: 2048
            );
            settings.WithSimulatorStageParameters(
                maxPacketCount: 4096,
                maxPacketSize: 1400,
                packetDelayMs: 20,
                packetJitterMs: 2
            );

            drivers[i] = NetworkDriver.Create(settings);
            simPipelines[i] = drivers[i].CreatePipeline(typeof(SimulatorPipelineStage));
        }

        csvLog.AppendLine("Timestamp,ActiveBots,RttSamples,MedianRTT_Ms,P95RTT_Ms,P99RTT_Ms,HandoffCount,MeanHandoff_Ms");
        logPath = Path.Combine(Application.persistentDataPath, $"swarm_metrics_{targetBotCount}bots.csv");
    }

    void Update()
    {
        if (drivers == null) return;

        // 1. Staggered spawn up to targetBotCount
        if (bots.Count < targetBotCount)
        {
            spawnAccumulator += spawnRatePerSecond * Time.deltaTime;
            while (spawnAccumulator >= 1f && bots.Count < targetBotCount)
            {
                SpawnVirtualBot();
                spawnAccumulator -= 1f;
            }
        }

        // 2. Advance all pooled drivers and drain 100% of events
        for (int d = 0; d < pooledDriverCount; d++)
        {
            if (!drivers[d].IsCreated) continue;
            drivers[d].ScheduleUpdate().Complete();
            DrainDriverEvents(d);
        }

        // 3. Step every bot's connection state machine and send movement input
        for (int i = 0; i < bots.Count; i++)
        {
            UpdateBot(bots[i]);
        }

        // 4. Periodically aggregate & flush latency metrics to CSV
        if (Time.time >= nextLogTime)
        {
            FlushMetricsWindow();
            nextLogTime = Time.time + 1.0f;
        }

        // 5. Optional zero-GameObject GPU Instanced rendering
        if (enableVisualization && botMesh != null && botMaterial != null && bots.Count > 0)
        {
            RenderBotsInstanced();
        }
    }

    private void SpawnVirtualBot()
    {
        int id = nextBotId++;
        int driverIdx = bots.Count % pooledDriverCount;

        float spawnX = Random.Range(-maxX, maxX);
        if (Mathf.Abs(spawnX) < 0.5f) spawnX = (spawnX < 0f) ? -0.5f : 0.5f;

        var bot = new VirtualBot
        {
            playerId = id,
            driverIndex = driverIdx,
            currentPosition = new Vector2(spawnX, Random.Range(-maxZ, maxZ))
        };

        PickNewDirection(bot);

        if (bot.currentPosition.x < 0f)
        {
            bot.serverConn = drivers[driverIdx].Connect(NetworkEndpoint.Parse(serverIp, region0Port));
            bot.activeRegionIndex = 0;
        }
        else
        {
            bot.serverConn = drivers[driverIdx].Connect(NetworkEndpoint.Parse(serverIp, region1Port));
            bot.activeRegionIndex = 1;
        }

        bots.Add(bot);
        botLookup[id] = bot;
    }

    private void DrainDriverEvents(int driverIdx)
    {
        var drv = drivers[driverIdx];
        DataStreamReader stream;
        NetworkConnection conn;
        NetworkEvent.Type cmd;

        while ((cmd = drv.PopEvent(out conn, out stream)) != NetworkEvent.Type.Empty)
        {
            if (cmd == NetworkEvent.Type.Data)
            {
                PacketType type = (PacketType)stream.ReadByte();

                if (type == PacketType.WorldSnapshot)
                {
                    int count = stream.ReadInt();
                    for (int i = 0; i < count; i++)
                    {
                        PlayerState state = MeshProtocol.ReadPlayerState(ref stream);

                        // Only update the specific bot owned by this connection & driver
                        if (botLookup.TryGetValue(state.PlayerId, out VirtualBot bot) &&
                            bot.driverIndex == driverIdx &&
                            bot.serverConn == conn &&
                            !bot.handoffInProgress)
                        {
                            bot.currentPosition = state.Position;

                            if (state.EchoClientTimestamp > 0)
                            {
                                double rttMs = (Time.realtimeSinceStartupAsDouble - state.EchoClientTimestamp) * 1000.0;
                                rttWindowSamples.Add(rttMs);
                            }
                        }
                    }
                }
                else if (type == PacketType.AuthorityHandoff)
                {
                    int newRegion = stream.ReadInt();
                    int targetPlayerId = stream.ReadInt();

                    if (botLookup.TryGetValue(targetPlayerId, out VirtualBot bot) &&
                        bot.driverIndex == driverIdx &&
                        bot.serverConn == conn &&
                        !bot.handoffInProgress)
                    {
                        bot.handoffInProgress = true;
                        bot.handoffStartTime = Time.realtimeSinceStartup;
                        bot.activeRegionIndex = newRegion;

                        ushort newPort = (newRegion == 0) ? region0Port : region1Port;
                        bot.pendingConn = drv.Connect(NetworkEndpoint.Parse(serverIp, newPort));
                    }
                }
            }
            else if (cmd == NetworkEvent.Type.Disconnect)
            {
                // Clear matching connection reference on the owning bot
                for (int i = 0; i < bots.Count; i++)
                {
                    var b = bots[i];
                    if (b.driverIndex != driverIdx) continue;
                    if (b.serverConn == conn) b.serverConn = default;
                    if (b.pendingConn == conn) b.pendingConn = default;
                    if (b.retiringConn == conn) b.retiringConn = default;
                }
            }
        }
    }

    private void UpdateBot(VirtualBot bot)
    {
        var drv = drivers[bot.driverIndex];

        // 1. Handoff timeout recovery
        if (bot.handoffInProgress && Time.realtimeSinceStartup - bot.handoffStartTime > 1.0f)
        {
            bot.handoffInProgress = false;
            if (bot.pendingConn.IsCreated)
            {
                drv.Disconnect(bot.pendingConn);
                bot.pendingConn = default;
            }
        }

        // 2. Complete staged handoff once pendingConn connects
        if (bot.pendingConn.IsCreated && drv.GetConnectionState(bot.pendingConn) == NetworkConnection.State.Connected)
        {
            double handoffMs = (Time.realtimeSinceStartup - bot.handoffStartTime) * 1000.0;
handoffWindowSamples.Add(handoffMs);
if (bot.retiringConn.IsCreated)
drv.Disconnect(bot.retiringConn);
if (bot.serverConn.IsCreated && drv.GetConnectionState(bot.serverConn) == NetworkConnection.State.Connected)
{
if (drv.BeginSend(NetworkPipeline.Null, bot.serverConn, out DataStreamWriter uw) >= 0)
{
uw.WriteByte((byte)PacketType.ClientUnregister);
drv.EndSend(uw);
}
}
bot.retiringConn = bot.serverConn;
bot.retiringConnTime = Time.time + 0.06f;
bot.serverConn = bot.pendingConn;
bot.pendingConn = default;
bot.connectedToActiveServer = false;
bot.handoffInProgress = false;
}
// 3. Close retiring socket after server delay queue drains
if (bot.retiringConn.IsCreated && Time.time >= bot.retiringConnTime)
{
drv.Disconnect(bot.retiringConn);
bot.retiringConn = default;
}
// 4. Send registration or movement input
if (!bot.serverConn.IsCreated || bot.handoffInProgress)
return;
if (drv.GetConnectionState(bot.serverConn) != NetworkConnection.State.Connected)
return;
if (!bot.connectedToActiveServer)
{
if (drv.BeginSend(NetworkPipeline.Null, bot.serverConn, out DataStreamWriter w) >= 0)
{
w.WriteByte((byte)PacketType.ClientRegistration);
w.WriteInt(bot.playerId);
w.WriteFloat(bot.currentPosition.x);
w.WriteFloat(bot.currentPosition.y);
drv.EndSend(w);
bot.connectedToActiveServer = true;
}
return;
}
if (Time.time >= bot.nextDirectionChangeTime)
PickNewDirection(bot);
if (bot.currentPosition.x > maxX) bot.moveDir.x = -Mathf.Abs(bot.moveDir.x);
if (bot.currentPosition.x < -maxX) bot.moveDir.x = Mathf.Abs(bot.moveDir.x);
if (bot.currentPosition.y > maxZ) bot.moveDir.y = -Mathf.Abs(bot.moveDir.y);
if (bot.currentPosition.y < -maxZ) bot.moveDir.y = Mathf.Abs(bot.moveDir.y);
bot.inputSeq++;
if (drv.BeginSend(NetworkPipeline.Null, bot.serverConn, out DataStreamWriter iw) >= 0)
{
iw.WriteByte((byte)PacketType.ClientInput);
iw.WriteInt(bot.playerId);
iw.WriteUInt(bot.inputSeq);
iw.WriteFloat(bot.moveDir.x);
iw.WriteFloat(bot.moveDir.y);
iw.WriteDouble(Time.realtimeSinceStartupAsDouble);
drv.EndSend(iw);
}
}
private void PickNewDirection(VirtualBot bot)
{
float angle = Random.Range(0f, Mathf.PI * 2f);
bot.moveDir = new Vector2(Mathf.Cos(angle), Mathf.Sin(angle)).normalized;
bot.nextDirectionChangeTime = Time.time + Random.Range(1.5f, 4.0f);
}
private void FlushMetricsWindow()
{
if (rttWindowSamples.Count == 0) return;
rttWindowSamples.Sort();
int n = rttWindowSamples.Count;
double p50 = rttWindowSamples[n / 2];
double p95 = rttWindowSamples[Mathf.Clamp((int)(n * 0.95f), 0, n - 1)];
double p99 = rttWindowSamples[Mathf.Clamp((int)(n * 0.99f), 0, n - 1)];
double meanHandoff = 0.0;
if (handoffWindowSamples.Count > 0)
{
for (int i = 0; i < handoffWindowSamples.Count; i++)
meanHandoff += handoffWindowSamples[i];
meanHandoff /= handoffWindowSamples.Count;
}
csvLog.AppendLine($"{Time.time:F2},{bots.Count},{n},{p50:F2},{p95:F2},{p99:F2},{handoffWindowSamples.Count},{meanHandoff:F2}");
rttWindowSamples.Clear();
handoffWindowSamples.Clear();
}
private void RenderBotsInstanced()
{
int renderCount = Mathf.Min(bots.Count, Mathf.Min(maxVisualizedBots, 1023));
for (int i = 0; i < renderCount; i++)
{
Vector2 p = bots[i].currentPosition;
renderMatrices[i] = Matrix4x4.TRS(new Vector3(p.x, 1f, p.y), Quaternion.identity, Vector3.one);
}
Graphics.DrawMeshInstanced(botMesh, 0, botMaterial, renderMatrices, renderCount);
}
void OnDestroy()
{
FlushMetricsWindow();
if (!string.IsNullOrEmpty(logPath))
File.WriteAllText(logPath, csvLog.ToString());
if (drivers != null)
{
for (int i = 0; i < drivers.Length; i++)
{
if (drivers[i].IsCreated) drivers[i].Dispose();
}
}
}
}