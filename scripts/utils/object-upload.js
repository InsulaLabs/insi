// Example usage: insio run scripts/utils/object-upload.js /path/to/your/file.txt

if (args.length !== 1) {
    console.error("Usage: object.upload <filepath>");
    throw new Error("Invalid number of arguments for object upload");
}

var filePath = args[0];
console.log("Attempting to upload file: " + filePath);

try {
    var response = object.upload(filePath);
    console.log("Upload successful!");
    console.log("ObjectID: " + response.objectID);
    if (response.message) {
        console.log("Message: " + response.message);
    }
} catch (e) {
    console.error("Upload failed: " + e.toString());
    throw e; // Re-throw to make the script exit with an error code
}
