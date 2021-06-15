node ('saturn') {

    try {    
    	def _branch = "master"
    	def _build = false
    	def _checkout = false 
        def _deploytoNexus = false 
		
        if(params.containsKey("Branch")){
            _branch=params.Branch
        }
        
        if(params.containsKey("Checkout")){
    		_checkout = params.Checkout
    	}
        
    	if(params.containsKey("Build")){
    		_build = params.Build
    	}
        if(params.containsKey("DeployToNexus")){
		   _deployToNexus = params.DeployToNexus
	    }
            	
	    echo "branch=$_branch" 
        echo "checkout=$_checkout"
        echo "build=$_build"
        echo "deployToNexus=$_deployToNexus"

                
    	if(_checkout){
    		stage ('Checkout'){
    		    checkout([$class: 'GitSCM', branches: [[name: "*/$_branch"]], doGenerateSubmoduleConfigurations: false, extensions: [], submoduleCfg: [], userRemoteConfigs: [[credentialsId: '22d9e0ef-8f5f-44f2-8df0-302f20c39b5b', url: 'http://gobitbucket.havelsan.com.tr/scm/link11-poc/custom-kafka-test-utils.git']]])
    		}
    	}
    	
	
    	
    	if(_build) {
    	    
    	    
    		stage ('Build'){
    			withEnv(["JAVA_HOME=${ tool 'jdk8' }", "PATH+MAVEN=${tool 'maven-3.3.3'}/bin:${ tool 'jdk8' }/bin"]){
    				sh "mvn clean install -U -f ./pom.xml  -settings /opt/apache-maven/apache-maven-3.3.3/conf/settings-link11-poc.xml"
 				
    			}
    		}
    		
    		stage ('SQ'){
			   

        }
	    
             
    }
      
      if(_deployToNexus){
          stage ('DeployToNexus'){
               withEnv(["JAVA_HOME=${ tool 'jdk8' }", "PATH+MAVEN=${tool 'maven-3.3.3'}/bin:${ tool 'jdk8' }/bin"]){
    		    		sh "mvn deploy -U -f ./pom.xml  -settings /opt/apache-maven/apache-maven-3.3.3/conf/settings-link11-poc.xml"			
               }
          }
      }

    }
           
     catch(Exception ex){
        echo "setting status to FAILURE"
        currentBuild.result = "FAILURE"
        echo "throwing exception..."
        println currentBuild.result
        throw ex;        
    } finally{
		
    }
}
