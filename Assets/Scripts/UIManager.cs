
using UnityEngine;
using UnityEngine.UI;
using TMPro;

public class CapsuleUI : MonoBehaviour
{
    public TMP_InputField capsuleInput;
    public Button applyButton;
    public CapsuleManager capsuleManager;

    void Start()
    {
        applyButton.onClick.AddListener(ApplyCapsuleCount);
    }

    public void ApplyCapsuleCount()
    {
        if (int.TryParse(capsuleInput.text, out int count))
        {
            capsuleManager.SetCapsuleCount(count);
        }
        else
        {
            Debug.LogWarning("Please enter a valid number.");
        }
    }
}
