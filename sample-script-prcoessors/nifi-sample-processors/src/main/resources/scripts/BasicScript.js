var flowFile = session.get();
if (flowFile != null) {
  session.transfer(flowFile, REL_SUCCESS);
}
