#!/usr/bin/env groovy

import net.sf.json.JSONArray
import net.sf.json.JSONObject

author = ""
message = ""
channel = "#project-cannoli"

def getGitAuthor = {
    def commit = sh(returnStdout: true, script: 'git rev-parse HEAD')
    author = sh(returnStdout: true, script: "git --no-pager show -s --format='%an' ${commit}").trim()
}

def getLastCommitMessage = {
    message = sh(returnStdout: true, script: 'git log -1 --pretty=%B').trim()
}

def populateGlobalVariables = {
    getLastCommitMessage()
    getGitAuthor()
    println author
    println message
}

def notifySlack = {
    JSONArray attachments = new JSONArray();
    JSONObject attachment = new JSONObject();
    JSONArray fields = new JSONArray();

    JSONObject authorField = new JSONObject();
    authorField.put("title", "Author")
    authorField.put("value", author)

    JSONObject msgField = new JSONObject();
    msgField.put("title", "Last commit")
    msgField.put("value", message)

    JSONObject jobField = new JSONObject();
    jobField.put("title", "Job name")
    jobField.put("value", env.JOB_NAME)

    JSONObject linkField = new JSONObject();
    linkField.put("title", "Jenkins link")
    linkField.put("value", env.BUILD_URL.replace("http://", "http://www."))

    JSONObject statusField = new JSONObject();
    statusField.put("title", "Build status")
    statusField.put("value", buildStatus)

    fields.add(jobField)
    fields.add(authorField)
    fields.add(msgField)
    fields.add(linkField)
    fields.add(statusField)

    attachment.put('text', "Another great piece of code has been tested...");
    attachment.put('fallback', 'Hey, Vader seems to be mad at you.');
    attachment.put("fields", fields)
    attachment.put('color', buildColor);
    attachments.add(attachment);

    slackSend(bot:false, channel: channel, attachments: attachments.toString())
}

node {

    stage('Checkout') {
        checkout scm
    }

    populateGlobalVariables()

    try {
        stage('Test Scala code') {
            echo 'Testing Scala code....'
            sh "SBT_OPTS='-XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=2G -Xmx2G' ${tool name: 'sbt-0.13.15', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt clean test"
        }
        stage('Package scala code') {
            echo 'Building Scala code....'
            sh "${tool name: 'sbt-0.13.15', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt package"
        }
         stage('Publish to Nexus snapshots') {
            echo "branch: ${env.BRANCH_NAME}"
            echo 'Publishing to ZSI-BIO snapshots repository....'
            sh "SBT_OPTS='-XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=2G -Xmx2G' ${tool name: 'sbt-0.13.15', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt 'set test in publish := {}' publish"
        }
        stage('Code stats') {
           echo 'Gathering code stats....'
           sh "${tool name: 'sbt-0.13.15', type: 'org.jvnet.hudson.plugins.SbtPluginBuilder$SbtInstallation'}/bin/sbt stats"
        }
    } catch (e) {
        currentBuild.result = "FAIL"
    }

    stage('Notify') {
        junit '**/target/test-reports/*.xml'
        buildColor = currentBuild.result == null ? "good" : "danger"
        buildStatus = currentBuild.result == null ? "SUCCESS:clap:" : currentBuild.result + ":cry:"
        notifySlack()
    }
}