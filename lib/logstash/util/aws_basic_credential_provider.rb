# encoding: utf-8
module LogStash::Util
  class AwsBasicCredentialProvider

    # Implement the IRecordProcessor interface
    include Java::ComAmazonawsAuth::AWSCredentialsProvider

    def initialize(access_key_id, secret_key)
      @credential = Java::ComAmazonawsAuth::BasicAWSCredentials.new(access_key_id,secret_key)
    end

    # public AWSCredentials getCredentials()
    def getCredentials
      @credential
    end

    def refresh(); end
  end
end
