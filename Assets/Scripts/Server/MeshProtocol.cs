/*
 * This is a custom packet for 
 * communication between servers
 * It is minimal, but operates purely with
 * the goal of transfering a basic
 * player state during spatial
 * boundary handoffs.
 */

using Unity.Collections;
using Unity.Networking.Transport;
using UnityEngine;

public enum PacketType : byte
{
    ClientInput = 1,
    WorldSnapshot = 2,
    GhostSync = 3,
    AuthorityHandoff = 4,
    ClientRegistration = 5 
}

public struct PlayerState
{
    public int PlayerId;
    public Vector2 Position;
    public uint LastProcessedInputSeq;
    public double EchoClientTimestamp;
    public bool IsGhost;
}

public static class MeshProtocol
{
    public static void WritePlayerState(ref DataStreamWriter writer, in PlayerState state)
    {
        writer.WriteInt(state.PlayerId);
        writer.WriteFloat(state.Position.x);
        writer.WriteFloat(state.Position.y);
        writer.WriteUInt(state.LastProcessedInputSeq);
        writer.WriteDouble(state.EchoClientTimestamp);
    }

    public static PlayerState ReadPlayerState(ref DataStreamReader reader, bool isGhost = false)
    {
        return new PlayerState
        {
            PlayerId = reader.ReadInt(),
            Position = new Vector2(reader.ReadFloat(), reader.ReadFloat()),
            LastProcessedInputSeq = reader.ReadUInt(),
            EchoClientTimestamp = reader.ReadDouble(),
            IsGhost = isGhost
        };
    }
}