#!groovy

@Library('katsdpjenkins') _
katsdp.killOldJobs()

katsdp.setDependencies([
    'ska-sa/katsdpdockerbase/new-rdma-core',
    'ska-sa/katsdpservices/master',
    'ska-sa/katsdptelstate/master'])
katsdp.standardBuild(python2: false, python3: true, push_external: true,
                     katsdpdockerbase_ref: 'new-rdma-core')
katsdp.mail('sdpdev+katsdpbfingest@ska.ac.za')
