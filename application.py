import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from watson_developer_cloud import AlchemyLanguageV1,WatsonException
import boto3


#alchemy_language = AlchemyLanguageV1(api_key='6be2f24db30c9b2e9bdc542f396033d6d82922d4')

#alchemy_language = AlchemyLanguageV1(api_key='a2692d701b17bdb9a9ca9718acce5cb3e03959b6')

alchemy_language = AlchemyLanguageV1(api_key='61593ab7df66780046e3873af48e74b9eb126756') #tejasshah


try:

    consumer = KafkaConsumer('test',bootstrap_servers=['localhost:9092'],value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    while True:
        for msg in consumer:
            #print ("%s:%d:%d: key=%s value=%s" % (msg.topic, msg.partition,msg.offset, msg.key,msg.value))
            jsonData = msg.value
            value = json.dumps(jsonData)

            if value:
                data = jsonData.get('text').encode('utf-8', 'ignore')
                #print jsonData['lang'].encode('utf-8') + " = " + data


                response = json.loads(json.dumps(alchemy_language.sentiment(text=data, language='english')))

                sentiment = response['docSentiment']['type']
                #sentiment = 'positive'

                jsonData['sentiment'] = sentiment
                print response['status'] + ' - ' + jsonData['sentiment']

                sns = boto3.client('sns')
                response1 = sns.publish(TopicArn='arn:aws:sns:us-west-2:801481767682:TwitterTrends', Message=value)
                print response1


except KafkaError:
    print KafkaError
    pass
except WatsonException as e:
    print 'Exception' + " - " + e.message
    pass
except Exception as e :
    print e.message
    pass
