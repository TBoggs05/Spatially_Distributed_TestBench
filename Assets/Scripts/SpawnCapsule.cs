
using UnityEngine;

public class RandomCapsuleSpawner : MonoBehaviour
{
    public float spawnRangeX = 10f;
    public float spawnRangeZ = 10f;
    public float spawnHeight = 1f;

    void Start()
    {
        SpawnCapsule();
    }

    void SpawnCapsule()
    {
        float randomX = Random.Range(-spawnRangeX, spawnRangeX);
        float randomZ = Random.Range(-spawnRangeZ, spawnRangeZ);

        Vector3 spawnPosition = new Vector3(
            randomX,
            spawnHeight,
            randomZ
        );

        GameObject capsule = GameObject.CreatePrimitive(PrimitiveType.Capsule);
        capsule.transform.position = spawnPosition;
    }
}
