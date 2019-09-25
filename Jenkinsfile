#!groovy

@Library('katsdpjenkins') _
katsdp.killOldJobs()

katsdp.setDependencies([
    'ska-sa/katsdpdockerbase/master',
    'ska-sa/katsdpservices/master',
    'ska-sa/katsdptelstate/master'])
katsdp.standardBuild(python2: false, python3: true, push_external: true)
katsdp.mail('sdpdev+katsdpbfingest@ska.ac.za')
