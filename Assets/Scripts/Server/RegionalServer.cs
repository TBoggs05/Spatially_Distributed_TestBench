using System.Collections.Generic;
using Unity.Collections;                 // Required for NativeList (unmanaged memory used by Unity Jobs/Transport)
using Unity.Networking.Transport;        // Core Unity Transport API (NetworkDriver, NetworkConnection, DataStream)
using Unity.Networking.Transport.Utilities; // Provides SimulatorPipelineStage for artificial latency/jitter injection
using UnityEngine;

public class RegionalServer : MonoBehaviour
{
    // =========================================================================
    // 1. INSPECTOR & COMMAND-LINE CONFIGURATION
    // =========================================================================

    [Header("Server Identity")]
    public int regionId = 0;               // Spatial region index: 0 = West (X < 0), 1 = East (X >= 0)
    public ushort publicClientPort = 7777; // UDP port where player/bot clients connect to this server
    public ushort peerMeshPort = 8777;     // Private UDP port where adjacent regional servers connect

    [Header("Neighbor Server (Mesh Link)")]
    public string neighborIp = "127.0.0.1"; // IP address of the neighboring region server
    public ushort neighborMeshPort = 8778;  // Private mesh port of the neighboring region server

    [Header("Simulation & Artificial Latency")]
    public float moveSpeed = 10f;          // Authoritative movement speed in world units per second
    public float bufferZoneWidth = 10f;     // Distance from X=0 where entities replicate as Ghosts to the neighbor
    public int simulatedLatencyMs = 20;    // One-way artificial delay (ms) injected on outbound client packets

    // =========================================================================
    // 2. NETWORK DRIVERS & CONNECTION STATE
    // =========================================================================

    private NetworkDriver clientDriver;                 // Low-level UDP socket manager handling all player clients
    private NetworkDriver meshDriver;                   // Dedicated low-level UDP socket manager for server-to-server traffic
    private NativeList<NetworkConnection> clientConnections; // Unmanaged dynamic array tracking active client sockets
    private NetworkConnection neighborConnection;       // Direct socket handle to the adjacent regional server

    // =========================================================================
    // 3. AUTHORITATIVE WORLD STATE (IN-MEMORY RAM)
    // =========================================================================

    // Players currently inside this server's spatial region (this server owns & simulates their movement)
    private Dictionary<int, PlayerState> authPlayers = new Dictionary<int, PlayerState>();

    // Bidirectional lookup tables mapping raw UDP connections <-> logical Player IDs
    private Dictionary<NetworkConnection, int> connToPlayerId = new Dictionary<NetworkConnection, int>();
    private Dictionary<int, NetworkConnection> playerIdToConn = new Dictionary<int, NetworkConnection>();

    // Read-only proxy players owned by the neighbor server, replicated here because they are near the border
    private Dictionary<int, PlayerState> ghostPlayers = new Dictionary<int, PlayerState>();

    // Pipeline ID (Pipeline 1) configured with SimulatorPipelineStage to delay outbound client snapshots
    private NetworkPipeline simPipeline;

    // =========================================================================
    // 4. INITIALIZATION (RUNS ONCE ON STARTUP)
    // =========================================================================

    void Start()
    {
        // Lock the headless/editor simulation loop to 60 ticks per second
        Application.targetFrameRate = 60;

        // Parse command-line flags (e.g. "-region 1 -port 7778") so multiple headless builds can share one binary
        string[] args = System.Environment.GetCommandLineArgs();
        for (int i = 0; i < args.Length - 1; i++)
        {
            if (args[i] == "-region" && int.TryParse(args[i + 1], out int r)) regionId = r;
            if (args[i] == "-port" && ushort.TryParse(args[i + 1], out ushort p)) publicClientPort = p;
            if (args[i] == "-meshPort" && ushort.TryParse(args[i + 1], out ushort mp)) peerMeshPort = mp;
            if (args[i] == "-neighborPort" && ushort.TryParse(args[i + 1], out ushort np)) neighborMeshPort = np;
        }

        // Configure internal packet queue capacities and artificial network latency parameters
        var clientSettings = new NetworkSettings();
        clientSettings.WithNetworkConfigParameters(
            sendQueueCapacity: 4096,       // Max outbound packets buffered before dropping
            receiveQueueCapacity: 4096     // Max inbound packets buffered per tick
        );
        clientSettings.WithSimulatorStageParameters(
            maxPacketCount: 10000,         // Max packets held in the artificial delay queue simultaneously
            maxPacketSize: 1400,           // Max bytes per packet (matches standard Ethernet/UDP MTU limit)
            packetDelayMs: simulatedLatencyMs, // Artificial latency added to each packet
            packetJitterMs: 2              // Random ±2 ms variance to simulate real network jitter
        );

        // Create the public client driver, attach the simulator pipeline (Pipeline 1), and bind to its UDP port
        clientDriver = NetworkDriver.Create(clientSettings);
        simPipeline = clientDriver.CreatePipeline(typeof(SimulatorPipelineStage));
        clientDriver.Bind(NetworkEndpoint.AnyIpv4.WithPort(publicClientPort));
        clientDriver.Listen(); // Start listening for incoming client handshakes

        // Create the private server-to-server driver with zero artificial delay (fast data-center interconnect)
        meshDriver = NetworkDriver.Create();
        meshDriver.Bind(NetworkEndpoint.AnyIpv4.WithPort(peerMeshPort));
        meshDriver.Listen(); // Start listening for peer server connections

        // Allocate persistent native memory for tracking active client connections across frames
        clientConnections = new NativeList<NetworkConnection>(256, Allocator.Persistent);
    }

    // =========================================================================
    // 5. MAIN SERVER TICK LOOP (DETERMINISTIC FIXED TIMESTEP)
    // =========================================================================

    void FixedUpdate()
    {
        if (!clientDriver.IsCreated || !meshDriver.IsCreated) return;

        // 1. Advance network drivers
        clientDriver.ScheduleUpdate().Complete();
        meshDriver.ScheduleUpdate().Complete();

        // 2. Drain 100% of events from BOTH drivers using driver-level PopEvent
        MaintainNeighborConnection();
        DrainClientDriver();
        DrainMeshDriver();

        // 3. Run simulation borders and broadcast snapshots
        CheckBordersAndSyncGhosts();
        BroadcastWorldSnapshots();
    }

    // =========================================================================
    // 6. STAGE 1 & 2: CONNECTION MANAGEMENT
    // =========================================================================

    private void MaintainNeighborConnection()
    {
        // Reset connection handle if the neighbor server disconnected or timed out
        if (neighborConnection.IsCreated && meshDriver.GetConnectionState(neighborConnection) == NetworkConnection.State.Disconnected)
            neighborConnection = default;

        // Accept any incoming server-to-server mesh handshake (typically used by Region 1 accepting Region 0)
        NetworkConnection incoming;
        while ((incoming = meshDriver.Accept()) != default)
        {
            if (!neighborConnection.IsCreated)
                neighborConnection = incoming;
        }

        // Region 0 acts as the dialer and initiates the outbound connection to Region 1's mesh port
        if (!neighborConnection.IsCreated && regionId < 1)
        {
            var ep = NetworkEndpoint.Parse(neighborIp, neighborMeshPort);
            neighborConnection = meshDriver.Connect(ep);
        }
    }

    private void DrainClientDriver()
    {
        // Compact disconnected slots
        for (int i = clientConnections.Length - 1; i >= 0; i--)
        {
            if (!clientConnections[i].IsCreated || clientDriver.GetConnectionState(clientConnections[i]) == NetworkConnection.State.Disconnected)
                clientConnections.RemoveAtSwapBack(i);
        }

        // Accept all incoming client handshakes
        NetworkConnection incoming;
        while ((incoming = clientDriver.Accept()) != default)
        {
            clientConnections.Add(incoming);
        }

        // Drain EVERY queued event across ALL client connections
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
                    // Guard: Only delete player state if this socket is still their active connection
                    if (playerIdToConn.TryGetValue(pId, out NetworkConnection activeConn) && activeConn == conn)
                    {
                        playerIdToConn.Remove(pId);
                        authPlayers.Remove(pId);
                        ghostPlayers.Remove(pId);
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

        // Drain EVERY queued event on the server-to-server mesh driver
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
                        ghostPlayers[ghost.PlayerId] = ghost;
                }
                else if (type == PacketType.AuthorityHandoff)
                {
                    PlayerState promoted = MeshProtocol.ReadPlayerState(ref stream);
                    promoted.IsGhost = false;
                    ghostPlayers.Remove(promoted.PlayerId);
                    authPlayers[promoted.PlayerId] = promoted;
                }
            }
        }
    }
    // =========================================================================
    // 9. STAGE 5: BORDER BUFFER SYNC & AUTHORITY HANDOFF
    // =========================================================================
    private void CheckBordersAndSyncGhosts()
{
    // Cannot sync ghosts or hand off players if the server-to-server mesh link isn't active
    if (!neighborConnection.IsCreated || meshDriver.GetConnectionState(neighborConnection) != NetworkConnection.State.Connected)
        return;
    // Track IDs handed off this tick (cannot remove from authPlayers while iterating over it)
    List<int> handedOffIds = new List<int>();
    foreach (var kvp in authPlayers)
    {
        PlayerState p = kvp.Value;
            // Check if the player crossed the X = 0 dividing line into the neighboring region
            bool crossedToEast = (regionId == 0 && p.Position.x >= 0.25f); //0.25 buffer to prevent jittering across border
            bool crossedToWest = (regionId == 1 && p.Position.x <= -0.25f); //0.25 buffer to prevent jittering across border

        if (crossedToEast || crossedToWest)
        {
            int targetRegion = (regionId == 0) ? 1 : 0;
            // Step A: Serialize player's live state and send AuthorityHandoff to the neighbor server
            meshDriver.BeginSend(NetworkPipeline.Null, neighborConnection, out DataStreamWriter w);
            w.WriteByte((byte)PacketType.AuthorityHandoff);
            MeshProtocol.WritePlayerState(ref w, in p);
            meshDriver.EndSend(w);
            // Step B: Instruct the player's client to open a connection to the new authoritative region
            if (playerIdToConn.TryGetValue(p.PlayerId, out NetworkConnection clientConn))
            {
                clientDriver.BeginSend(simPipeline, clientConn, out DataStreamWriter cw);
                cw.WriteByte((byte)PacketType.AuthorityHandoff);
                cw.WriteInt(targetRegion);
                cw.WriteInt(p.PlayerId);
                clientDriver.EndSend(cw);
            }
            // Step C: Demote our local copy to a Ghost so local clients still see them near the border
            p.IsGhost = true;
            ghostPlayers[p.PlayerId] = p;
            handedOffIds.Add(p.PlayerId);
        }
        else if (Mathf.Abs(p.Position.x) <= bufferZoneWidth)
        {
            // Player hasn't crossed yet, but is inside the buffer zone: replicate Ghost state to neighbor
            meshDriver.BeginSend(NetworkPipeline.Null, neighborConnection, out DataStreamWriter w);
            w.WriteByte((byte)PacketType.GhostSync);
            MeshProtocol.WritePlayerState(ref w, in p);
            meshDriver.EndSend(w);
        }
    }
    // Remove handed-off players from our authoritative simulation dictionary
    foreach (int id in handedOffIds)
        authPlayers.Remove(id);
}
// =========================================================================
// 10. STAGE 6: WORLD SNAPSHOT BROADCASTING (WITH MTU PACKET CHUNKING)
// =========================================================================
private void BroadcastWorldSnapshots()
{
    int totalEntities = authPlayers.Count + ghostPlayers.Count;
    if (totalEntities == 0) return;
    // Combine local authoritative entities and border ghost entities into one flat list
    List<PlayerState> allEntities = new List<PlayerState>(totalEntities);
    allEntities.AddRange(authPlayers.Values);
    allEntities.AddRange(ghostPlayers.Values);
    // Batch 40 entities per packet (40 * 25 bytes = 1,000 bytes) to stay safely below the 1,400-byte UDP MTU
    const int maxEntitiesPerPacket = 40;
    for (int i = 0; i < clientConnections.Length; i++)
    {
        var conn = clientConnections[i];
        if (!conn.IsCreated || clientDriver.GetConnectionState(conn) != NetworkConnection.State.Connected)
            continue;
        // Slice the entity list into 40-entity chunks and send each chunk as its own packet
        for (int offset = 0; offset < allEntities.Count; offset += maxEntitiesPerPacket)
        {
            int chunkCount = Mathf.Min(maxEntitiesPerPacket, allEntities.Count - offset);
            if (clientDriver.BeginSend(simPipeline, conn, out DataStreamWriter w) < 0)
                break; // Abort if the outbound queue is temporarily saturated
            w.WriteByte((byte)PacketType.WorldSnapshot);
            w.WriteInt(chunkCount);
            for (int j = 0; j < chunkCount; j++)
                MeshProtocol.WritePlayerState(ref w, allEntities[offset + j]);
            clientDriver.EndSend(w);
        }
    }
}
// =========================================================================
// 11. CLEANUP (PREVENT NATIVE MEMORY LEAKS ON SHUTDOWN)
// =========================================================================
void OnDestroy()
{
    // Unmanaged Unity.Collections and NetworkDrivers must be manually disposed when the server closes
    if (clientDriver.IsCreated) clientDriver.Dispose();
    if (meshDriver.IsCreated) meshDriver.Dispose();
    if (clientConnections.IsCreated) clientConnections.Dispose();
}
}