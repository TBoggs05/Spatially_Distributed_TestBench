using System.Collections.Generic;
using Unity.Collections;
using Unity.Networking.Transport;
using Unity.Networking.Transport.Utilities;
using UnityEngine;

public class RegionalServer : MonoBehaviour
{
    [Header("Server Identity")]
    public int regionId = 0; // 0 = West (X < 0), 1 = East (X >= 0)
    public ushort publicClientPort = 7777;
    public ushort peerMeshPort = 8777;

    [Header("Neighbor Server (Mesh Link)")]
    public string neighborIp = "127.0.0.1";
    public ushort neighborMeshPort = 8778;

    [Header("Simulation & Artificial Latency")]
    public float moveSpeed = 10f;
    public float bufferZoneWidth = 5f;
    public int simulatedLatencyMs = 20;

    private NetworkDriver clientDriver;
    private NetworkDriver meshDriver;
    private NativeList<NetworkConnection> clientConnections;
    private NetworkConnection neighborConnection;

    private Dictionary<int, PlayerState> authPlayers = new Dictionary<int, PlayerState>();
    private Dictionary<NetworkConnection, int> connToPlayerId = new Dictionary<NetworkConnection, int>();
    private Dictionary<int, NetworkConnection> playerIdToConn = new Dictionary<int, NetworkConnection>();
    private Dictionary<int, PlayerState> ghostPlayers = new Dictionary<int, PlayerState>();

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
        clientSettings.WithSimulatorStageParameters(
            maxPacketCount: 1000,
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

        MaintainNeighborConnection();
        AcceptNewClients();
        ProcessClientPackets();
        ProcessMeshPackets();
        CheckBordersAndSyncGhosts();
        BroadcastWorldSnapshots();
    }

    private void MaintainNeighborConnection()
    {
        if (neighborConnection.IsCreated && meshDriver.GetConnectionState(neighborConnection) == NetworkConnection.State.Disconnected)
            neighborConnection = default;

        NetworkConnection incoming;
        while ((incoming = meshDriver.Accept()) != default)
        {
            if (!neighborConnection.IsCreated)
                neighborConnection = incoming;
        }

        if (!neighborConnection.IsCreated && regionId < 1)
        {
            var ep = NetworkEndpoint.Parse(neighborIp, neighborMeshPort);
            neighborConnection = meshDriver.Connect(ep);
        }
    }

    private void AcceptNewClients()
    {
        for (int i = clientConnections.Length - 1; i >= 0; i--)
        {
            if (!clientConnections[i].IsCreated)
                clientConnections.RemoveAtSwapBack(i);
        }

        NetworkConnection c;
        while ((c = clientDriver.Accept()) != default)
            clientConnections.Add(c);
    }

    private void ProcessClientPackets()
    {
        for (int i = 0; i < clientConnections.Length; i++)
        {
            var conn = clientConnections[i];
            DataStreamReader stream;
            NetworkEvent.Type cmd;

            while ((cmd = clientDriver.PopEventForConnection(conn, out stream)) != NetworkEvent.Type.Empty)
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

                        // Spawn fresh ONLY if this player wasn't already handed off via Mesh link
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
                        // Only remove player data if they haven't already been handed off to neighbor
                        authPlayers.Remove(pId);
                        playerIdToConn.Remove(pId);
                        connToPlayerId.Remove(conn);
                    }
                    clientConnections[i] = default;
                }
            }
        }
    }

    private void ProcessMeshPackets()
    {
        if (!neighborConnection.IsCreated) return;

        DataStreamReader stream;
        NetworkEvent.Type cmd;
        while ((cmd = meshDriver.PopEventForConnection(neighborConnection, out stream)) != NetworkEvent.Type.Empty)
        {
            if (cmd == NetworkEvent.Type.Disconnect)
            {
                neighborConnection = default;
                break;
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

    private void CheckBordersAndSyncGhosts()
    {
        if (!neighborConnection.IsCreated || meshDriver.GetConnectionState(neighborConnection) != NetworkConnection.State.Connected)
            return;

        List<int> handedOffIds = new List<int>();

        foreach (var kvp in authPlayers)
        {
            PlayerState p = kvp.Value;
            bool crossedToEast = (regionId == 0 && p.Position.x >= 0f);
            bool crossedToWest = (regionId == 1 && p.Position.x < 0f);

            if (crossedToEast || crossedToWest)
            {
                int targetRegion = (regionId == 0) ? 1 : 0;

                // 1. Transfer authoritative state to neighbor server over Mesh link
                meshDriver.BeginSend(NetworkPipeline.Null, neighborConnection, out DataStreamWriter w);
                w.WriteByte((byte)PacketType.AuthorityHandoff);
                MeshProtocol.WritePlayerState(ref w, in p);
                meshDriver.EndSend(w);

                // 2. Tell client to connect to targetRegion
                if (playerIdToConn.TryGetValue(p.PlayerId, out NetworkConnection clientConn))
                {
                    clientDriver.BeginSend(simPipeline, clientConn, out DataStreamWriter cw);
                    cw.WriteByte((byte)PacketType.AuthorityHandoff);
                    cw.WriteInt(targetRegion);
                    cw.WriteInt(p.PlayerId);
                    clientDriver.EndSend(cw);
                }

                // 3. Demote local entity to Ghost
                p.IsGhost = true;
                ghostPlayers[p.PlayerId] = p;
                handedOffIds.Add(p.PlayerId);
            }
            else if (Mathf.Abs(p.Position.x) <= bufferZoneWidth)
            {
                meshDriver.BeginSend(NetworkPipeline.Null, neighborConnection, out DataStreamWriter w);
                w.WriteByte((byte)PacketType.GhostSync);
                MeshProtocol.WritePlayerState(ref w, in p);
                meshDriver.EndSend(w);
            }
        }
        foreach (int id in handedOffIds)
            authPlayers.Remove(id);
    }
    private void BroadcastWorldSnapshots()
    {
        int totalEntities = authPlayers.Count + ghostPlayers.Count;
        if (totalEntities == 0) return;
        for (int i = 0; i < clientConnections.Length; i++)
        {
            var conn = clientConnections[i];
            if (!conn.IsCreated || clientDriver.GetConnectionState(conn) != NetworkConnection.State.Connected)
                continue;
            clientDriver.BeginSend(simPipeline, conn, out DataStreamWriter w);
            w.WriteByte((byte)PacketType.WorldSnapshot);
            w.WriteInt(totalEntities);
            foreach (var p in authPlayers.Values)
                MeshProtocol.WritePlayerState(ref w, in p);
            foreach (var g in ghostPlayers.Values)
                MeshProtocol.WritePlayerState(ref w, in g);
            clientDriver.EndSend(w);
        }
    }
    void OnDestroy()
    {
        if (clientDriver.IsCreated) clientDriver.Dispose();
        if (meshDriver.IsCreated) meshDriver.Dispose();
        if (clientConnections.IsCreated) clientConnections.Dispose();
    }
}