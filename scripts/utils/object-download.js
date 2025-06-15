// Example usage: insio run scripts/utils/object-download.js <object-uuid> /path/to/save/file.txt

if (args.length !== 2) {
    console.error("Usage: object.download <uuid> <output_path>");
    throw new Error("Invalid number of arguments for object download");
}

var objectId = args[0];
var outputPath = args[1];

console.log("Attempting to download object " + objectId + " to " + outputPath);

try {
    object.download(objectId, outputPath);
    console.log("Download successful!");
} catch (e) {
    console.error("Download failed: " + e.toString());
    throw e; // Re-throw to make the script exit with an error code
}
