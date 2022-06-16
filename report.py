import pika
import json

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
queue = channel.queue_declare('order_report')
queue_name = queue.method.queue
channel.queue_bind(exchange='order',
                   queue=queue_name,
                   routing_key='order.report'  # binding key
                   )


def callback(ch, method, properties, body):
    payload = json.loads(body)
    print(' [x] generating report')
    print(f"""
    ID: {payload.get('id')}
    User Email: {payload.get('user_email')} 
    Product: {payload.get('product')} 
    Quantity: {payload.get('quantity')}
    """)
    print('[x] Done')
    print("checking for head in gt history")
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_consume(on_message_callback=callback, queue=queue_name)
print('[*] waiting for notify messages.To exit pres Ctril c')
channel.start_consuming()
