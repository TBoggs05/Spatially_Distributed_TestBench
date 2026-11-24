using UnityEngine;
using System.Collections.Generic;

public class CapsuleManager : MonoBehaviour
{
    [Header("Prefab")]
    public GameObject capsulePrefab;

    [Header("Spawn Area")]
    public float spawnRangeX = 50f;
    public float spawnRangeZ = 50f;
    public float spawnHeight = 1f;

    //Currently active capsules
    private List<GameObject> activeCapsules = new List<GameObject>();

    // Inactive capsules available for reuse
    private Stack<GameObject> capsulePool = new Stack<GameObject>();

    public int ActiveCount => activeCapsules.Count;

    public void SetCapsuleCount(int targetCount)
    {
        targetCount = Mathf.Max(0, targetCount);

        //Spawn or retrieve capsules from the pool
        while (activeCapsules.Count < targetCount)
        {
            SpawnCapsule();
        }
        while (activeCapsules.Count > targetCount)
        {
            RemoveCapsule();
        }

        Debug.Log($"Capsule count : {activeCapsules.Count}");
    }

    private void SpawnCapsule()
    {
        GameObject capsule;

        if (capsulePool.Count > 0)
        {
            // Reuse an existing capsule
            capsule = capsulePool.Pop();
        }
        else
        {
            // Create a new capsule
            capsule = Instantiate(capsulePrefab, transform);
        }

        // Generate random position
        float x = Random.Range(-spawnRangeX, spawnRangeX);
        float z = Random.Range(-spawnRangeZ, spawnRangeZ);

        Vector3 position = 
            new Vector3(x, spawnHeight, z);

        capsule.transform.position = position;
        capsule.transform.rotation = Quaternion.identity;
        capsule.SetActive(true);

        activeCapsules.Add(capsule);
    }

    private void RemoveCapsule()
    {
        int lastIndex = activeCapsules.Count - 1;

        GameObject capsule = activeCapsules[lastIndex];

        activeCapsules.RemoveAt(lastIndex);

        // Disable instead of destroying
        capsule.SetActive(false);

        capsulePool.Push(capsule);
    }
}
