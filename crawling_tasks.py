from celery import Celery
import os
import asyncio
import sys
from datetime import datetime
import json
import pika

from crawler.bhx.demo import main as bhx_main
from crawler.winmart.demo import main as winmart_main
from dotenv import load_dotenv

load_dotenv()

broker_url = os.getenv('RABBITMQ_URL') 

# Celery app cho crawling service  
celery_app = Celery('crawling_service', broker=broker_url)

celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    broker_connection_retry_on_startup=True,
    # Windows-specific settings
    worker_pool='solo',
    worker_disable_rate_limits=True,
    task_ignore_result=True,
    worker_send_task_events=False,
)

@celery_app.task(bind=True)
def crawl_bhx_store_task(self, task_id, store_id, province_id=3, ward_id=4946, district_id=0, concurrency=3):
    """Celery task for BHX crawling"""
    print(f"🚀 Starting BHX crawl: {task_id}, store: {store_id}")
    
    try:
        send_status_update(task_id, 'processing')
        
        if sys.platform.startswith("win"):
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        
        # Create new event loop for this task
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            result = loop.run_until_complete(bhx_main(concurrency, store_id, province_id, ward_id, district_id))
        finally:
            loop.close()
        
        # Send completion status
        if result.get('status') == 'success':
            send_status_update(task_id, 'completed', result)
            print(f"✅ BHX crawl completed: {task_id}")
        else:
            send_status_update(task_id, 'failed', error=result.get('error'))
            print(f"❌ BHX crawl failed: {task_id}")
        
        return result
        
    except Exception as e:
        error_msg = str(e)
        print(f"❌ BHX task error: {task_id} - {error_msg}")
        send_status_update(task_id, 'failed', error=error_msg)
        return {'status': 'error', 'error': error_msg}

@celery_app.task(bind=True)  
def crawl_winmart_store_task(self, task_id, store_code, concurrency=2):
    """Celery task for WinMart crawling"""
    print(f"🚀 Starting WinMart crawl: {task_id}, store: {store_code}")
    
    try:
        send_status_update(task_id, 'processing')
        
        if sys.platform.startswith("win"):
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        
        # Create new event loop for this task
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            result = loop.run_until_complete(winmart_main(concurrency, store_code))
        finally:
            loop.close()
        
        if result.get('status') == 'success':
            send_status_update(task_id, 'completed', result)
            print(f"✅ WinMart crawl completed: {task_id}")
        else:
            send_status_update(task_id, 'failed', error=result.get('error'))
            print(f"❌ WinMart crawl failed: {task_id}")
        
        return result
        
    except Exception as e:
        error_msg = str(e)
        print(f"❌ WinMart task error: {task_id} - {error_msg}")
        send_status_update(task_id, 'failed', error=error_msg)
        return {'status': 'error', 'error': error_msg}

def send_status_update(task_id, status, result=None, error=None):
    """Send status update to main service via RabbitMQ"""
    try:
        rabbitmq_url = os.getenv('RABBITMQ_URL')
        response_queue = os.getenv('RABBITMQ_CRAWLING_RESPONSE_QUEUE')
        
        connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_url))
        channel = connection.channel()
        channel.queue_declare(queue=response_queue, durable=True)
        
        status_message = {
            'action': 'task_status_update',
            'task_id': task_id,
            'status': status,
            'timestamp': datetime.utcnow().isoformat(),
            'service': 'celery_worker'
        }
        
        if result:
            status_message['result'] = result
        if error:
            status_message['error'] = error
        
        channel.basic_publish(
            exchange='',
            routing_key=response_queue,
            body=json.dumps(status_message, ensure_ascii=False, default=str),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        
        connection.close()
        
    except Exception as e:
        print(f"❌ Status update failed: {task_id} - {e}")