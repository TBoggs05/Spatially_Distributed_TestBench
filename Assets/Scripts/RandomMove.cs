
using UnityEngine;

public class RandomMove : MonoBehaviour
{
    [Header("Movement")]
    public float moveSpeed = 2f;
    public float arrivalDistance = 0.1f;

    [Header("Movement Area")]
    public float rangeX = 50f;
    public float rangeZ = 50f;

    private Vector3 targetPosition;

    void Start()
    {
        PickNewDestination();
    }

    void Update()
    {
        // Move toward destination
        transform.position = Vector3.MoveTowards(
            transform.position,
            targetPosition,
            moveSpeed * Time.deltaTime
        );

        // Check if destination reached
        if (Vector3.Distance(transform.position, targetPosition)
            < arrivalDistance)
        {
            PickNewDestination();
        }
    }

    void PickNewDestination()
    {
        float randomX = Random.Range(-rangeX, rangeX);
        float randomZ = Random.Range(-rangeZ, rangeZ);

        targetPosition = new Vector3(
            randomX,
            transform.position.y,
            randomZ
        );
    }
}
