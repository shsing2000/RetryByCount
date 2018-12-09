/**
 * Simple read and write JSON example:
 * In: { "id": 5 }
 * Out: { "id": 5, "count": 1 }
 */

var flowFile = session.get();
if (flowFile != null) {
  var StreamCallback =  Java.type("org.apache.nifi.processor.io.StreamCallback")
  var IOUtils = Java.type("org.apache.commons.io.IOUtils")
  var StandardCharsets = Java.type("java.nio.charset.StandardCharsets")
  var errorMessage = null
  var transferRelationship = REL_SUCCESS

  flowFile = session.write(flowFile,
    new StreamCallback(function(inputStream, outputStream) {
      var jsonTxt = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
      try {
        var item = JSON.parse(jsonTxt)
        item.count = 1
        outputStream.write(JSON.stringify(item, null, 2).getBytes(StandardCharsets.UTF_8))
      } catch (e) {
        errorMessage = e.message
        transferRelationship = REL_FAILURE
      }
    }))

  if (errorMessage) {
    flowFile = session.putAttribute(flowFile, "error.message", errorMessage)
  }

  session.transfer(flowFile, transferRelationship)
}
