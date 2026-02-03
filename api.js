const KEY = "9ADB5C6C-A9B0-11EF-A261-42010A80000F";

export async function fetchPurpleAir(sensorId) {
    const url = `https://api.purpleair.com/v1/sensors/${sensorId}?api_key=${KEY}`;
    // Vercel handles this fetch much more securely than GitHub
    const response = await fetch(url);
    return await response.json();
}
