const {Producer} = require("rib-kafka");
const UUID = require("uuid/v1");
async function runProducer () {
    let producer = new Producer({
        config: {"metadata.broker.list": "192.168.0.185:9092"} // конфиги продюсера librdkafka
    });

    try {
        await producer.connect();

        producer.on("error", (err) => {
            console.error(err);
        }); // обязательно подписываемся на событие error
    } catch (err) {
        console.error(err); // ошибка при подключении
        return;
    }

    let tt =   {

        "MS_ID" : 123,
        "component_name" : "m2-1",
        "status" : "IN_QUEUE" ,
        data:

    {

        "msisdn" : [ ],
        "date_from": "2006-06-06" ,

        "date_to": "2006-06-06",

        "entities": ["call.count"],

        "entity_type": "SYSTEM_INDEX",

        "force": true,

        "mei": [ ]

    }

}

    let Id = UUID();
    let sent_msg = await producer.produce(
        "task-test", // название топика для записи
        0, // номер партиции. Можно передать null, тогда партиция будет выбрана случайным образом
        new Buffer(JSON.stringify(tt)), // Обязательно в типе Bufferza
        new Buffer(Id), // key. Обязательно в типе Buffer
        Date.now() // timestamp
    );

    console.log(sent_msg); // sent_msg => {topic, partition - выбранная партиция (если не была задана вручную), offset - оффсет записанного сообщения, key, value, timestamp}

    producer.disconnect(); // чтобы закрыть соединение с брокером
}

runProducer().catch(e => console.log('err: ', e));