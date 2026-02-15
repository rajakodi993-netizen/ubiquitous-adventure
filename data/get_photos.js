const { TiktokDL } = require('@tobyg74/tiktok-api-dl');
const url = process.argv[2];

if (!url) {
    console.error("Please provide a URL");
    process.exit(1);
}

// Versi API bisa 'v1', 'v2', 'v3'. Coba v1 dulu yang paling stabil.
TiktokDL(url, { version: "v1" }).then((result) => {
    // Output JSON murni agar mudah diparse Python
    console.log(JSON.stringify(result));
}).catch((err) => {
    // Jangan print ke stdout agar tidak merusak JSON parsing Python
    console.error(err);
    process.exit(1);
});
