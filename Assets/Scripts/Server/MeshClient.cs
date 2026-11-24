using System.IO;
using System.Text;
using Unity.Collections;
using Unity.Networking.Transport;
using Unity.Networking.Transport.Utilities;
using UnityEngine;

public class MeshClient : MonoBehaviour
{
    [Header("Server Endpoints")]
    public string serverIp = "127.0.0.1";
    public ushort region0Port = 7777;
    public ushort region1Port = 7778;

    [Header("Benchmark Settings")]
    public bool isHeadlessBot = false;
    public int myPlayerId = -1;

    [Header("Map Boundaries")]
    public float maxX = 10f;
    public float maxZ = 10f;

    private NetworkDriver driver;
    private NetworkConnection serverConn;
    private NetworkConnection pendingConn = default;
    private uint inputSeq = 0;

    private Vector2 currentPosition;
    private Vector2 botMoveDir;
    private float nextDirectionChangeTime = 0f;

    private NetworkPipeline simPipeline;
    private StringBuilder csvLog = new StringBuilder();
    private string logPath;

    private bool connectedToActiveServer = false;
    private int activeRegionIndex;
    private bool handoffInProgress = false;

    void Start()
    {
        if (myPlayerId < 0) myPlayerId = Random.Range(1000, 999999);

        // Assign class field (no 'Vector2' type prefix) and avoid spawning exactly on X = 0
        float spawnX = Random.Range(-maxX, maxX);
        if (Mathf.Abs(spawnX) < 0.5f) spawnX = (spawnX < 0f) ? -0.5f : 0.5f;
        currentPosition = new Vector2(spawnX, Random.Range(-maxZ, maxZ));
        transform.position = new Vector3(currentPosition.x, 0f, currentPosition.y);

        PickNewBotDirection();

        var settings = new NetworkSettings();
        settings.WithSimulatorStageParameters(
            maxPacketCount: 1000,
            maxPacketSize: 1400,
            packetDelayMs: 20,
            packetJitterMs: 2
        );

        driver = NetworkDriver.Create(settings);
        simPipeline = driver.CreatePipeline(typeof(SimulatorPipelineStage));

        if (currentPosition.x < 0f)
        {
            serverConn = driver.Connect(NetworkEndpoint.Parse(serverIp, region0Port));
            activeRegionIndex = 0;
        }
        else
        {
            serverConn = driver.Connect(NetworkEndpoint.Parse(serverIp, region1Port));
            activeRegionIndex = 1;
        }

        csvLog.AppendLine("Timestamp,ActiveRegion,PlayerX,PlayerZ,RTT_Ms");
        logPath = Path.Combine(Application.persistentDataPath, $"rtt_log_{myPlayerId}.csv");
    }

    void Update()
    {
        if (!driver.IsCreated) return;
        driver.ScheduleUpdate().Complete();

        if (serverConn.IsCreated) PollConnection(serverConn);
        if (pendingConn.IsCreated) PollConnection(pendingConn);

        // Swap staged connection once handshake completes
        if (pendingConn.IsCreated && driver.GetConnectionState(pendingConn) == NetworkConnection.State.Connected)
        {
            if (serverConn.IsCreated)
                driver.Disconnect(serverConn);

            serverConn = pendingConn;
            pendingConn = default;
            connectedToActiveServer = false;
            handoffInProgress = false;
        }

        SendMovementInput();
        transform.position = new Vector3(currentPosition.x, 0f, currentPosition.y);
    }

    private void PollConnection(NetworkConnection conn)
    {
        DataStreamReader stream;
        NetworkEvent.Type cmd;
        while ((cmd = driver.PopEventForConnection(conn, out stream)) != NetworkEvent.Type.Empty)
        {
            if (cmd == NetworkEvent.Type.Data)
            {
                PacketType type = (PacketType)stream.ReadByte();

                if (handoffInProgress || conn != serverConn)
                    continue;

                if (type == PacketType.WorldSnapshot)
                {
                    int count = stream.ReadInt();
                    for (int i = 0; i < count; i++)
                    {
                        PlayerState state = MeshProtocol.ReadPlayerState(ref stream);
                        if (state.PlayerId == myPlayerId)
                        {
                            currentPosition = state.Position;
                            if (state.EchoClientTimestamp > 0)
                            {
                                double rttMs = (Time.realtimeSinceStartupAsDouble - state.EchoClientTimestamp) * 1000.0;
                                csvLog.AppendLine($"{Time.time:F3},{activeRegionIndex},{currentPosition.x:F2},{currentPosition.y:F2},{rttMs:F2}");
                            }
                        }
                    }
                }
                else if (type == PacketType.AuthorityHandoff)
                {
                    int newRegion = stream.ReadInt();
                    int targetPlayerId = stream.ReadInt();
                    if (targetPlayerId == myPlayerId && !handoffInProgress)
                    {
                        handoffInProgress = true;
                        activeRegionIndex = newRegion;
                        ushort newPort = (newRegion == 0) ? region0Port : region1Port;
                        pendingConn = driver.Connect(NetworkEndpoint.Parse(serverIp, newPort));
                    }
                }
            }
            else if (cmd == NetworkEvent.Type.Disconnect)
            {
                if (conn == serverConn) serverConn = default;
                if (conn == pendingConn) pendingConn = default;
                break;
            }
        }
    }

    private void SendMovementInput()
    {
        if (!serverConn.IsCreated || handoffInProgress)
            return;

        if (driver.GetConnectionState(serverConn) != NetworkConnection.State.Connected)
            return;

        // Send initial registration with chosen random spawn position
        if (!connectedToActiveServer)
        {
            driver.BeginSend(NetworkPipeline.Null, serverConn, out DataStreamWriter w);
            w.WriteByte((byte)PacketType.ClientRegistration);
            w.WriteInt(myPlayerId);
            w.WriteFloat(currentPosition.x);
            w.WriteFloat(currentPosition.y);
            driver.EndSend(w);
            connectedToActiveServer = true;
            return;
        }

        Vector2 move;
        if (isHeadlessBot)
        {
            if (Time.time >= nextDirectionChangeTime)
                PickNewBotDirection();

            // Bounce cleanly off outer map boundaries
            if (currentPosition.x > maxX) botMoveDir.x = -Mathf.Abs(botMoveDir.x);
            if (currentPosition.x < -maxX) botMoveDir.x = Mathf.Abs(botMoveDir.x);
            if (currentPosition.y > maxZ) botMoveDir.y = -Mathf.Abs(botMoveDir.y);
            if (currentPosition.y < -maxZ) botMoveDir.y = Mathf.Abs(botMoveDir.y);

            move = botMoveDir;
        }
        else
        {
            move = new Vector2(Input.GetAxisRaw("Horizontal"), Input.GetAxisRaw("Vertical"));
        }

        if (move != Vector2.zero)
        {
            inputSeq++;
            driver.BeginSend(NetworkPipeline.Null, serverConn, out DataStreamWriter w);
            w.WriteByte((byte)PacketType.ClientInput);
            w.WriteInt(myPlayerId);
            w.WriteUInt(inputSeq);
            w.WriteFloat(move.x);
            w.WriteFloat(move.y);
            w.WriteDouble(Time.realtimeSinceStartupAsDouble);
            driver.EndSend(w);
        }
    }

    private void PickNewBotDirection()
    {
        float angle = Random.Range(0f, Mathf.PI * 2f);
        botMoveDir = new Vector2(Mathf.Cos(angle), Mathf.Sin(angle)).normalized;
        nextDirectionChangeTime = Time.time + Random.Range(1.5f, 4.0f);
    }

    void OnDestroy()
    {
        if (!string.IsNullOrEmpty(logPath))
            File.WriteAllText(logPath, csvLog.ToString());
        if (driver.IsCreated) driver.Dispose();
    }
}
