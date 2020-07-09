# A function to set up a test, since it's the same for each one.  Note:
# unit_tests test is not set up using this function because it does not like
# the additional argument on commmand line and consequently does not run any
# test.
function(tubemq_add_example _name)
  set(_srcs ${ARGN})
  message (STATUS "${_name} sources: ${_srcs}")
  add_executable (${_name} ${_srcs})
  TARGET_LINK_LIBRARIES (${_name} tubemq log4cplus pthread)
endfunction()

add_subdirectory (log)
