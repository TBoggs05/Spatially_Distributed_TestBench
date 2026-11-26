using UnityEngine;
/*
 * This script uses AddForce() to roll the sphere entity towards some designated goal.
 * 
 */


public class RollSphere : MonoBehaviour
{
    [SerializeField] float strength = 2.5f;
    // Physics calls (forces) must belong in fixedupdate
    void FixedUpdate()
    {
        float moveHorizontal = Input.GetAxis("Horizontal");
        float moveVertical = Input.GetAxis("Vertical");
        Vector3 force = new Vector3(moveHorizontal * strength, 0, moveVertical* strength);
        Rigidbody rigidbody = GetComponent<Rigidbody>();
        rigidbody.AddForce(force);
    }
}
