# encoding: utf-8
require "logstash/codecs/base"
require "logstash/util/charset"
require 'protobuf-java-2.4.1'
require "logstash/util/logevent.jar"
require 'jprotobuf'


class LogStash::Codecs::Protobuf < LogStash::Codecs::Base
  config_name "protobuf"
  milestone 1

  public
  def register
    JProtobuf.load!('com.logstash.event')
  end

  public
  def encode(data)
    fields = data.to_hash.collect do |k,v| 
      Logeven::Field.create(
        key => k,
        value => v
      ) if not %w(@timestamp message tags).include?(k)
    end.flatten
    tags = data['tags']
    Logevent::Event.create(
      timestamp => data.fetch('@timestamp', Time.now).to_i,
      message => data['message'],
      fields => fields,
      tags => tags
    )
  end # def encode

  public
  def decode(data)
    fields = Hash[data.fields.map{|f| [f.key, f.value]}]
    fields['tags'] = data['tags']
    fileds['timestamp'] = data['timestamps']
    fields['message'] = data['message']
    yield LogStash::Event.new(
    )
  end # def decode

end # class LogStash::Codecs::Protobuf
