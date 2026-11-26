using System.Collections.Generic;
using Unity.Collections;
using Unity.Networking.Transport;
using Unity.Networking.Transport.Utilities;
using UnityEngine;

public class RegionalServer : MonoBehaviour
{
    [Header("Server Identity")]
    public int regionId = 0;        // 0 = West (X < 0), 1 = East (X >= 0)
    public ushort publicClientPort = 7777;
    public ushort peerMeshPort = 8777;

    [Header("Neighbor Server (Mesh Link)")]
    public string neighborIp = "127.0.0.1";
    public ushort neighborMeshPort = 8778;

    [Header("Simulation & Artificial Latency")]
    public float moveSpeed = 10f;
    public float bufferZoneWidth = 5f;
    public float handoffHysteresis = 0.25f;
    public int simulatedLatencyMs = 20;

    private NetworkDriver clientDriver;
    private NetworkDriver meshDriver;
    private NativeList<NetworkConnection> clientConnections;
    private NetworkConnection neighborConnection;

    private Dictionary<int, PlayerState> authPlayers = new Dictionary<int, PlayerState>();
    private Dictionary<NetworkConnection, int> connToPlayerId = new Dictionary<NetworkConnection, int>();
    private Dictionary<int, NetworkConnection> playerIdToConn = new Dictionary<int, NetworkConnection>();

    // Track ghost states and last update timestamps to expire stale border ghosts
    private Dictionary<int, PlayerState> ghostPlayers = new Dictionary<int, PlayerState>();
    private Dictionary<int, float> ghostLastSeen = new Dictionary<int, float>();

    private NetworkPipeline simPipeline;

    void Start()
    {
        Application.targetFrameRate = 60;

        string[] args = System.Environment.GetCommandLineArgs();
        for (int i = 0; i < args.Length - 1; i++)
        {
            if (args[i] == "-region" && int.TryParse(args[i + 1], out int r)) regionId = r;
            if (args[i] == "-port" && ushort.TryParse(args[i + 1], out ushort p)) publicClientPort = p;
            if (args[i] == "-meshPort" && ushort.TryParse(args[i + 1], out ushort mp)) peerMeshPort = mp;
            if (args[i] == "-neighborPort" && ushort.TryParse(args[i + 1], out ushort np)) neighborMeshPort = np;
        }

        var clientSettings = new NetworkSettings();
        clientSettings.WithNetworkConfigParameters(
            sendQueueCapacity: 4096,
     receiveQueueCapacity: 4096
        );
        clientSettings.WithSimulatorStageParameters(
            maxPacketCount: 10000,
            maxPacketSize: 1400,
            packetDelayMs: simulatedLatencyMs,
            packetJitterMs: 2
        );

        clientDriver = NetworkDriver.Create(clientSettings);
        simPipeline = clientDriver.CreatePipeline(typeof(SimulatorPipelineStage));
        clientDriver.Bind(NetworkEndpoint.AnyIpv4.WithPort(publicClientPort));
        clientDriver.Listen();

        meshDriver = NetworkDriver.Create();
        meshDriver.Bind(NetworkEndpoint.AnyIpv4.WithPort(peerMeshPort));
        meshDriver.Listen();

        clientConnections = new NativeList<NetworkConnection>(256, Allocator.Persistent);
    }
    void FixedUpdate()
    {
        if (!clientDriver.IsCreated || !meshDriver.IsCreated) return;

        clientDriver.ScheduleUpdate().Complete();
        meshDriver.ScheduleUpdate().Complete();
        DrainClientDriver();
        DrainMeshDriver();
        MaintainNeighborConnection();
        CheckBordersAndSyncGhosts();
        CleanupStaleGhosts();
        BroadcastWorldSnapshots();
    }

    private void MaintainNeighborConnection()
    {
        if (neighborConnection.IsCreated)
        {
            var state = meshDriver.GetConnectionState(neighborConnection);
            if (state != NetworkConnection.State.Connected && state != NetworkConnection.State.Connecting)
            {
                neighborConnection = default;
            }
        }

        NetworkConnection incoming;
        while ((incoming = meshDriver.Accept()) != default)
        {
            if (!neighborConnection.IsCreated)
                neighborConnection = incoming;
            else
                meshDriver.Disconnect(incoming); // Prevent dangling half-open connections
        }

        if (!neighborConnection.IsCreated && regionId < 1)
        {
            var ep = NetworkEndpoint.Parse(neighborIp, neighborMeshPort);
            neighborConnection = meshDriver.Connect(ep);
        }
    }


    private void DrainClientDriver()
    {
        // Compact dead client slots and purge stale ID mappings
        for (int i = clientConnections.Length - 1; i >= 0; i--)
        {
            var c = clientConnections[i];
            if (!c.IsCreated || clientDriver.GetConnectionState(c) == NetworkConnection.State.Disconnected)
            {
                if (connToPlayerId.TryGetValue(c, out int deadId))
                {
                    connToPlayerId.Remove(c);
                    if (playerIdToConn.TryGetValue(deadId, out var mapped) && mapped == c)
                        playerIdToConn.Remove(deadId);
                }
                clientConnections.RemoveAtSwapBack(i);
            }
        }

        NetworkConnection incoming;
        while ((incoming = clientDriver.Accept()) != default)
        {
            clientConnections.Add(incoming);
        }

        // Drain 100% of queued events across all client connections
        DataStreamReader stream;
        NetworkConnection conn;
        NetworkEvent.Type cmd;
        while ((cmd = clientDriver.PopEvent(out conn, out stream)) != NetworkEvent.Type.Empty)
        {
            if (cmd == NetworkEvent.Type.Data)
            {
                PacketType type = (PacketType)stream.ReadByte();

                if (type == PacketType.ClientRegistration)
                {
                    int pId = stream.ReadInt();
                    Vector2 requestedSpawn = new Vector2(stream.ReadFloat(), stream.ReadFloat());

                    connToPlayerId[conn] = pId;
                    playerIdToConn[pId] = conn;

                    if (!authPlayers.ContainsKey(pId) && !ghostPlayers.ContainsKey(pId))
                    {
                        authPlayers[pId] = new PlayerState
                        {
                            PlayerId = pId,
                            Position = requestedSpawn,
                            IsGhost = false
                        };
                    }
                }
                else if (type == PacketType.ClientInput)
                {
                    int pId = stream.ReadInt();
                    uint seq = stream.ReadUInt();
                    Vector2 input = new Vector2(stream.ReadFloat(), stream.ReadFloat()).normalized;
                    double timestamp = stream.ReadDouble();

                    connToPlayerId[conn] = pId;
                    playerIdToConn[pId] = conn;

                    if (authPlayers.TryGetValue(pId, out PlayerState state))
                    {
                        state.Position += input * moveSpeed * Time.fixedDeltaTime;
                        state.LastProcessedInputSeq = seq;
                        state.EchoClientTimestamp = timestamp;
                        state.IsGhost = false;
                        authPlayers[pId] = state;
                    }
                }
            }
            else if (cmd == NetworkEvent.Type.Disconnect)
            {
                if (connToPlayerId.TryGetValue(conn, out int pId))
                {
                    connToPlayerId.Remove(conn);
                    if (playerIdToConn.TryGetValue(pId, out NetworkConnection activeConn) && activeConn == conn)
                    {
                        playerIdToConn.Remove(pId);
                        authPlayers.Remove(pId);
                        ghostPlayers.Remove(pId);
                        ghostLastSeen.Remove(pId);
                    }
                }
            }
        }
    }

    private void DrainMeshDriver()
    {
        DataStreamReader stream;
        NetworkConnection conn;
        NetworkEvent.Type cmd;

        // Drain 100% of queued events on the server-to-server mesh driver
        while ((cmd = meshDriver.PopEvent(out conn, out stream)) != NetworkEvent.Type.Empty)
        {
            if (cmd == NetworkEvent.Type.Connect)
            {
                if (!neighborConnection.IsCreated)
                    neighborConnection = conn;
            }
            else if (cmd == NetworkEvent.Type.Disconnect)
            {
                if (conn == neighborConnection)
                    neighborConnection = default;
            }
            else if (cmd == NetworkEvent.Type.Data)
            {
                PacketType type = (PacketType)stream.ReadByte();
                if (type == PacketType.GhostSync)
                {
                    PlayerState ghost = MeshProtocol.ReadPlayerState(ref stream);
                    ghost.IsGhost = true;
                    if (!authPlayers.ContainsKey(ghost.PlayerId))
                    {
                        ghostPlayers[ghost.PlayerId] = ghost;
                        ghostLastSeen[ghost.PlayerId] = Time.time;
                    }
                }
                else if (type == PacketType.AuthorityHandoff)
                {
                    PlayerState promoted = MeshProtocol.ReadPlayerState(ref stream);
                    promoted.IsGhost = false;
                    ghostPlayers.Remove(promoted.PlayerId);
                    ghostLastSeen.Remove(promoted.PlayerId);
                    authPlayers[promoted.PlayerId] = promoted;
                }
            }
        }
    }

    private void CheckBordersAndSyncGhosts()
    {
        if (!neighborConnection.IsCreated || meshDriver.GetConnectionState(neighborConnection) != NetworkConnection.State.Connected)
            return;

        List<int> handedOffIds = new List<int>();

        foreach (var kvp in authPlayers)
        {
            PlayerState p = kvp.Value;
            bool crossedToEast = (regionId == 0 && p.Position.x >= handoffHysteresis);
            bool crossedToWest = (regionId == 1 && p.Position.x <= -handoffHysteresis);

            if (crossedToEast || crossedToWest)
            {
                int targetRegion = (regionId == 0) ? 1 : 0;
                if (meshDriver.BeginSend(NetworkPipeline.Null, neighborConnection, out DataStreamWriter w) >= 0)
                {
                    w.WriteByte((byte)PacketType.AuthorityHandoff);
                    MeshProtocol.WritePlayerState(ref w, in p);
                    meshDriver.EndSend(w);
                }
                if (playerIdToConn.TryGetValue(p.PlayerId, out NetworkConnection clientConn))
                {
                    if (clientConn.IsCreated && clientDriver.GetConnectionState(clientConn) == NetworkConnection.State.Connected)
                    {
                        if (clientDriver.BeginSend(simPipeline, clientConn, out DataStreamWriter cw) >= 0)
                        {
                            cw.WriteByte((byte)PacketType.AuthorityHandoff);
                            cw.WriteInt(targetRegion);
                            cw.WriteInt(p.PlayerId);
                            clientDriver.EndSend(cw);
                        }
                    }
                    else
                    {
                        playerIdToConn.Remove(p.PlayerId);
                    }
                }
                p.IsGhost = true;
                ghostPlayers[p.PlayerId] = p;
                ghostLastSeen[p.PlayerId] = Time.time;
                handedOffIds.Add(p.PlayerId);
            }
            else if (Mathf.Abs(p.Position.x) <= bufferZoneWidth)
            {
                if (meshDriver.BeginSend(NetworkPipeline.Null, neighborConnection, out DataStreamWriter w) >= 0)
                {
                    w.WriteByte((byte)PacketType.GhostSync);
                    MeshProtocol.WritePlayerState(ref w, in p);
                    meshDriver.EndSend(w);
                }
            }
        }
        foreach (int id in handedOffIds)
            authPlayers.Remove(id);
    }

    private void CleanupStaleGhosts()
    {
        if (ghostPlayers.Count == 0) return;
        List<int> expiredGhosts = new List<int>();
        foreach (var kvp in ghostLastSeen)
        {
            // Expire ghosts that haven't synced in 0.25s (moved outside buffer zone or disconnected)
            if (Time.time - kvp.Value > 0.25f)
                expiredGhosts.Add(kvp.Key);
        }
        foreach (int id in expiredGhosts)
        {
            ghostPlayers.Remove(id);
            ghostLastSeen.Remove(id);
        }
    }

    private void BroadcastWorldSnapshots()
    {
        int totalEntities = authPlayers.Count + ghostPlayers.Count;
        if (totalEntities == 0) return;
        List<PlayerState> allEntities = new List<PlayerState>(totalEntities);
        allEntities.AddRange(authPlayers.Values);
        allEntities.AddRange(ghostPlayers.Values);
        const int maxEntitiesPerPacket = 40;
        for (int i = 0; i < clientConnections.Length; i++)
        {
            var conn = clientConnections[i];
            if (!conn.IsCreated || clientDriver.GetConnectionState(conn) != NetworkConnection.State.Connected)
                continue;
            for (int offset = 0; offset < allEntities.Count; offset += maxEntitiesPerPacket)
            {
                int chunkCount = Mathf.Min(maxEntitiesPerPacket, allEntities.Count - offset);
                if (clientDriver.BeginSend(simPipeline, conn, out DataStreamWriter w) < 0)
                    break;
                w.WriteByte((byte)PacketType.WorldSnapshot);
                w.WriteInt(chunkCount);
                for (int j = 0; j < chunkCount; j++)
                    MeshProtocol.WritePlayerState(ref w, allEntities[offset + j]);
                clientDriver.EndSend(w);
            }
        }
    }

    void OnDestroy()
    {
        if (clientDriver.IsCreated) clientDriver.Dispose();
        if (meshDriver.IsCreated) meshDriver.Dispose();
        if (clientConnections.IsCreated) clientConnections.Dispose();
    }
}