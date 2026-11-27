using System;
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
    // Add a reusable field on RegionalServer to avoid per-tick GC allocations:
    private readonly List<PlayerState> authSnapshotBuffer = new();

    //Network Drivers, AKA, socket wrappers with event queues, pipelines, and C# job system integration. 
    private NetworkDriver clientDriver;
    private NetworkDriver meshDriver;
    //Network connections, each represents a single logical peer link. (client or server)
    private NativeList<NetworkConnection> clientConnections;
    private NetworkConnection neighborConnection; //Only have 2 servers in current setup.
    //players owned by the regional server (within borders)
    private Dictionary<int, PlayerState> authPlayers = new Dictionary<int, PlayerState>();
    private Dictionary<NetworkConnection, int> connToPlayerId = new Dictionary<NetworkConnection, int>();
    private Dictionary<int, NetworkConnection> playerIdToConn = new Dictionary<int, NetworkConnection>();

    // Players owned by the neighboring server that are close enough to X = 0 that local players should see them. Read only.
    private Dictionary<int, PlayerState> ghostPlayers = new Dictionary<int, PlayerState>();
    private Dictionary<int, float> ghostLastSeen = new Dictionary<int, float>();

    private NetworkPipeline simPipeline;

    private readonly Dictionary<NetworkConnection, double> pendingDisconnects = new();
    private readonly List<NetworkConnection> disconnectBuffer = new();

    // Set this slightly higher than your SimulatorPipelineStage (PacketDelayMs + PacketJitterMs)
    private const double SimDrainGraceSeconds = 0.3; 


    void Start()
    {
        Application.targetFrameRate = 60;
        //command line parsing
        string[] args = System.Environment.GetCommandLineArgs();
        for (int i = 0; i < args.Length - 1; i++)
        {
            if (args[i] == "-region" && int.TryParse(args[i + 1], out int r)) regionId = r;
            if (args[i] == "-port" && ushort.TryParse(args[i + 1], out ushort p)) publicClientPort = p;
            if (args[i] == "-meshPort" && ushort.TryParse(args[i + 1], out ushort mp)) peerMeshPort = mp;
            if (args[i] == "-neighborPort" && ushort.TryParse(args[i + 1], out ushort np)) neighborMeshPort = np;
        }

        var clientSettings = new NetworkSettings(); //configure the network driver before creation
        clientSettings.WithNetworkConfigParameters(
            sendQueueCapacity: 4096,
     receiveQueueCapacity: 4096
        );
        clientSettings.WithSimulatorStageParameters(
            maxPacketCount: 10000, 
            maxPacketSize: 1400, //transport MTU
            packetDelayMs: simulatedLatencyMs, //fake delay baseline
            packetJitterMs: 2 //fake jitter
        );

        clientDriver = NetworkDriver.Create(clientSettings); //initialize the network driver to handle clients
        simPipeline = clientDriver.CreatePipeline(typeof(SimulatorPipelineStage)); //pipeline that lets us inject artificial jitter, latency, packet loss
        clientDriver.Bind(NetworkEndpoint.AnyIpv4.WithPort(publicClientPort)); //bind socket
        clientDriver.Listen(); //open for connections

        meshDriver = NetworkDriver.Create(); //driver to talk between servers (orchestration)
        meshDriver.Bind(NetworkEndpoint.AnyIpv4.WithPort(peerMeshPort));
        meshDriver.Listen();
        //NetworkConnection - burst compatible and unmanaged => so we put it in container safely and pass to C# job workers
        clientConnections = new NativeList<NetworkConnection>(256, Allocator.Persistent); //A list of networkconnection structs for handling individual clients
    }
    void FixedUpdate()
    {
        if (!clientDriver.IsCreated || !meshDriver.IsCreated) return; //check to make sure network drivers have been created to avoid null refs
     
        ProcessPendingDisconnects();
        //schedule internal C# Job system network loop. Blocks main thread until all network I/O finishes.
        clientDriver.ScheduleUpdate().Complete();
        meshDriver.ScheduleUpdate().Complete();

        DrainClientDriver();
        DrainMeshDriver();
        
        MaintainNeighborConnection();
        CheckBordersAndSyncGhosts();
        CleanupStaleGhosts();
        BroadcastWorldSnapshots();
        
        
    }

    private void DrainClientDriver()
{
    // 1. Accept incoming connections first
    NetworkConnection incoming;
    while ((incoming = clientDriver.Accept()) != default)
    {
        clientConnections.Add(incoming);
    }

    // 2. Drain 100% of queued events BEFORE compacting dead connections
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

                // Only register if the connection hasn't already dropped
                if (conn.IsCreated && clientDriver.GetConnectionState(conn) == NetworkConnection.State.Connected)
                {
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
            }
            else if (type == PacketType.ClientInput)
            {
                int pId = stream.ReadInt();
                uint seq = stream.ReadUInt();
                Vector2 input = new Vector2(stream.ReadFloat(), stream.ReadFloat()).normalized;
                double timestamp = stream.ReadDouble();

                // Only accept input from the currently mapped, active connection for this player
                if (playerIdToConn.TryGetValue(pId, out NetworkConnection mappedConn) && mappedConn == conn)
                {
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
        }
        else if (cmd == NetworkEvent.Type.Disconnect)
        {
            CleanupPlayerConnection(conn);
        }
    }

    // 3. Compact dead client slots AFTER events are drained, ensuring full cleanup
    for (int i = clientConnections.Length - 1; i >= 0; i--)
    {
        var c = clientConnections[i];
        if (!c.IsCreated || clientDriver.GetConnectionState(c) == NetworkConnection.State.Disconnected)
        {
            CleanupPlayerConnection(c);
            clientConnections.RemoveAtSwapBack(i);
        }
    }
}

private void CleanupPlayerConnection(NetworkConnection conn)
{
     pendingDisconnects.Remove(conn);

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

    //handle the relevant server-to-server communication
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
    private void CheckBordersAndSyncGhosts()
    {
        if (!neighborConnection.IsCreated || meshDriver.GetConnectionState(neighborConnection) != NetworkConnection.State.Connected)
            return;

        //List<int> handedOffIds = new List<int>();
         authSnapshotBuffer.Clear();
         authSnapshotBuffer.AddRange(authPlayers.Values);
        for (int i = 0; i < authSnapshotBuffer.Count; i++)
        {
            PlayerState p = authSnapshotBuffer[i];
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
                        // Queue the graceful disconnect so simPipeline finishes flushing
                        // both the WorldSnapshots and this AuthorityHandoff packet before closing the socket.
                        ScheduleGracefulDisconnect(clientConn);
                    }
                    else
                    {
                        playerIdToConn.Remove(p.PlayerId);
                    }
                }
                p.IsGhost = true;
                ghostPlayers[p.PlayerId] = p;
                ghostLastSeen[p.PlayerId] = Time.time;
                //handedOffIds.Add(p.PlayerId);
            }
            //ok this is supposed to be the logic to send data to the other server if the client
            //is within the buffer zone boundary
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
        //foreach (int id in handedOffIds)
           // authPlayers.Remove(id);
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
            if (!conn.IsCreated || pendingDisconnects.ContainsKey(conn) ||clientDriver.GetConnectionState(conn) != NetworkConnection.State.Connected)
            {
                continue;
            }
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
    private void ScheduleGracefulDisconnect(NetworkConnection conn)
    {
        if (!conn.IsCreated || pendingDisconnects.ContainsKey(conn))
            return;

        // 1. Clean up game state immediately so no more snapshots/logic target this player
        CleanupPlayerConnection(conn);

        // 2. Defer the low-level driver disconnect until simPipeline drains
        pendingDisconnects[conn] = Time.realtimeSinceStartupAsDouble + SimDrainGraceSeconds;
    }
    private void ProcessPendingDisconnects()
    {
        if (pendingDisconnects.Count == 0) return;

        double now = Time.realtimeSinceStartupAsDouble;
        disconnectBuffer.Clear();

        foreach (var kvp in pendingDisconnects)
        {
            if (now >= kvp.Value)
                disconnectBuffer.Add(kvp.Key);
        }

        for (int i = 0; i < disconnectBuffer.Count; i++)
        {
            NetworkConnection conn = disconnectBuffer[i];
            pendingDisconnects.Remove(conn);

            if (conn.IsCreated && clientDriver.GetConnectionState(conn) != NetworkConnection.State.Disconnected)
            {
                clientDriver.Disconnect(conn);
                
            }
        }
    }
    //cleanup network drivers (close sockets and such)
    void OnDestroy()
    {
        if (clientDriver.IsCreated) clientDriver.Dispose();
        if (meshDriver.IsCreated) meshDriver.Dispose();
        if (clientConnections.IsCreated) clientConnections.Dispose();
    }
}