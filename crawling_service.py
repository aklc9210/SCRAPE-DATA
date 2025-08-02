# import pika
# import json
# import asyncio
# import time
# import sys
# from datetime import datetime
# import os
# from dotenv import load_dotenv
# import logging

# # Import BHX crawler components
# from crawler.bhx.demo import BHXDataFetcher
# from crawler.bhx.fetch_full_location import fetch_full_location_data
# from crawler.bhx.fetch_store_by_province import fetch_stores_async
# from crawler.process_data.process import CATEGORIES_MAPPING_BHX

# # Import WinMart crawler components
# from crawler.winmart.demo import WinMartFetcher
# from crawler.process_data.process import CATEGORIES_MAPPING_WINMART

# from db.db_async import get_db

# load_dotenv()

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

# class UniversalCrawlingService:
#     def __init__(self):
#         self.connection = None
#         self.channel = None
#         # Remove global fetchers - create fresh ones per request
#         self.db = None
        
#         # RabbitMQ config
#         self.rabbitmq_url = os.getenv('RABBITMQ_URL')
#         self.request_queue = os.getenv('RABBITMQ_CRAWLING_REQUEST_QUEUE')
#         self.response_queue = os.getenv('RABBITMQ_CRAWLING_RESPONSE_QUEUE')
        
#         self.supported_chains = ['BHX', 'WM', 'ALL']
    
#     async def initialize(self):
#         """Initialize database and RabbitMQ only"""
#         try:
#             if sys.platform.startswith("win"):
#                 asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
            
#             self.db = get_db()            
#             self.setup_rabbitmq_connection()
#             logger.info("✅ Service initialized successfully")
            
#         except Exception as e:
#             logger.error(f"❌ Init failed: {e}")
#             raise e
    
#     def setup_rabbitmq_connection(self):
#         """Setup RabbitMQ connection"""
#         try:
#             connection_params = pika.URLParameters(self.rabbitmq_url)
#             self.connection = pika.BlockingConnection(connection_params)
#             self.channel = self.connection.channel()
            
#             self.channel.queue_declare(queue=self.request_queue, durable=True)
#             self.channel.queue_declare(queue=self.response_queue, durable=True)
            
#             logger.info(f"✅ RabbitMQ connected")
            
#         except Exception as e:
#             logger.error(f"❌ RabbitMQ failed: {e}")
#             raise e
    
#     def process_crawling_request(self, ch, method, properties, body):
#         """Process incoming request"""
#         try:
#             request = json.loads(body)
#             correlation_id = request.get('correlationId')
#             action = request.get('action', 'unknown')
#             chain = request.get('chain', 'ALL').upper()
            
#             logger.info(f"📨 Request: {action} for {chain} (ID: {correlation_id})")
            
#             # Process in separate thread with fresh event loop
#             import threading
#             response_container = {}
            
#             def run_async_handler():
#                 # Create completely new event loop for this request
#                 new_loop = asyncio.new_event_loop()
#                 asyncio.set_event_loop(new_loop)
                
#                 if sys.platform.startswith("win"):
#                     asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
                
#                 try:
#                     response_container['result'] = new_loop.run_until_complete(
#                         self.handle_request_fresh(request)
#                     )
#                 except Exception as e:
#                     logger.error(f"❌ Error in async handler: {e}")
#                     response_container['result'] = {
#                         'action': action,
#                         'status': 'error',
#                         'error': str(e)
#                     }
#                 finally:
#                     new_loop.close()
            
#             thread = threading.Thread(target=run_async_handler)
#             thread.start()
#             thread.join(timeout=600)  # 10 minutes timeout
            
#             if thread.is_alive():
#                 response = {'action': action, 'status': 'error', 'error': 'Timeout'}
#             else:
#                 response = response_container.get('result', {
#                     'action': action, 'status': 'error', 'error': 'Unknown error'
#                 })
            
#             # Add metadata
#             response['correlationId'] = correlation_id
#             response['timestamp'] = datetime.utcnow().isoformat()
#             response['processedBy'] = 'UniversalCrawlingService'
            
#             self.send_response(response)
#             ch.basic_ack(delivery_tag=method.delivery_tag)
            
#         except Exception as e:
#             logger.error(f"❌ Process error: {e}")
#             ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    
#     async def handle_request_fresh(self, request: dict) -> dict:
#         """Handle request with fresh fetchers in new event loop"""
#         start_time = time.time()
#         action = request.get('action', 'unknown')
#         chain = request.get('chain', 'ALL').upper()
        
#         # Create fresh fetchers in this event loop
#         bhx_fetcher = None
#         winmart_fetcher = None
        
#         try:
#             if action == 'ping':
#                 response = await self.handle_ping_fresh(request)
#             # elif action == 'crawl_store':
#             #     response = await self.handle_crawl_store_fresh(request, chain)
#             # elif action == 'search_products':
#             #     response = await self.handle_search_products_fresh(request, chain)
#             else:
#                 response = self.handle_unknown_action(request)
            
#             response['processingTime'] = time.time() - start_time
#             response['chain'] = chain
#             return response
            
#         except Exception as e:
#             logger.error(f"❌ Error in handle_request_fresh: {e}")
#             return {
#                 'action': action,
#                 'chain': chain,
#                 'status': 'error',
#                 'error': str(e),
#                 'processingTime': time.time() - start_time
#             }
#         finally:
#             # Clean up fetchers
#             try:
#                 if bhx_fetcher:
#                     await bhx_fetcher.close()
#                 if winmart_fetcher:
#                     await winmart_fetcher.close()
#             except Exception as e:
#                 logger.warning(f"⚠️ Error closing fetchers: {e}")
    
#     async def handle_ping_fresh(self, request: dict) -> dict:
#         """Handle ping with fresh status check"""
#         return {
#             'action': 'ping',
#             'status': 'success',
#             'message': 'Pong from Universal Crawling Service',
#             'serviceInfo': {
#                 'name': 'Universal Crawling Service',
#                 'version': '2.0.0',
#                 'supportedChains': self.supported_chains,
#                 'capabilities': ['crawl_store', 'search_products']
#             },
#             'chainStatus': {
#                 'BHX': 'ready_to_initialize',
#                 'WM': 'ready_to_initialize'
#             }
#         }
    
#     def handle_unknown_action(self, request: dict) -> dict:
#         """Handle unknown action"""
#         return {
#             'action': request.get('action', 'unknown'),
#             'status': 'error',
#             'error': f'Unknown action: {request.get("action")}',
#             'supportedActions': ['ping', 'crawl_store', 'search_products'],
#             'supportedChains': self.supported_chains
#         }
    
#     def send_response(self, response: dict):
#         """Send response"""
#         try:
#             self.channel.basic_publish(
#                 exchange='',
#                 routing_key=self.response_queue,
#                 body=json.dumps(response, ensure_ascii=False, default=str),
#                 properties=pika.BasicProperties(delivery_mode=2, content_type='application/json')
#             )
#         except Exception as e:
#             logger.error(f"❌ Send response failed: {e}")
    
#     def start_consuming(self):
#         """Start consuming"""
#         logger.info("🔄 Service started. Waiting for requests...")
#         logger.info(f"   Supported: {self.supported_chains}")
#         logger.info("   Press CTRL+C to stop")
        
#         self.channel.basic_qos(prefetch_count=1)
#         self.channel.basic_consume(
#             queue=self.request_queue,
#             on_message_callback=self.process_crawling_request
#         )
        
#         try:
#             self.channel.start_consuming()
#         except KeyboardInterrupt:
#             logger.info("\n🛑 Stopping service...")
#             self.stop()
#         except Exception as e:
#             logger.error(f"❌ Consuming error: {e}")
#             self.stop()
    
#     def stop(self):
#         """Stop service"""
#         if self.channel:
#             self.channel.stop_consuming()
        
#         if self.connection and not self.connection.is_closed:
#             self.connection.close()
        
#         logger.info("✅ Service stopped")
    
#     async def close(self):
#         """Close connections"""
#         self.stop()

# async def main():
#     """Main function"""
#     service = None
#     try:
#         logger.info("🚀 Starting Universal Crawling Service...")
#         service = UniversalCrawlingService()
#         await service.initialize()
#         service.start_consuming()
#     except Exception as e:
#         logger.error(f"❌ Failed to start: {e}")
#     finally:
#         if service:
#             await service.close()

# if __name__ == "__main__":
#     asyncio.run(main())


import os
import json
import pika
import asyncio
from datetime import datetime

# Import các fetcher
from crawler.bhx.demo import run_sync as crawl_bhx_all, BHXDataFetcher
from crawler.winmart.demo import run_sync as crawl_wm_all, WinMartFetcher

# RabbitMQ config
RABBITMQ_URL  = os.getenv('RABBITMQ_URL', 'amqp://guest:guest@localhost:5672/%2f')
REQUEST_QUEUE = os.getenv('RABBITMQ_REQUEST_QUEUE', 'crawling_request')
RESPONSE_QUEUE= os.getenv('RABBITMQ_RESPONSE_QUEUE','crawling_response')

def send_response(corr_id, response):
    """Publish result back if cần hoặc ack"""
    params = pika.URLParameters(RABBITMQ_URL)
    conn   = pika.BlockingConnection(params)
    ch     = conn.channel()
    ch.queue_declare(queue=RESPONSE_QUEUE, durable=True)
    msg = {'correlationId': corr_id, **response}
    ch.basic_publish(exchange='', routing_key=RESPONSE_QUEUE,
                     body=json.dumps(msg, ensure_ascii=False),
                     properties=pika.BasicProperties(delivery_mode=2))
    conn.close()

def on_request(ch, method, props, body):
    req = json.loads(body)
    action = req.get('action')
    corr_id = req.get('correlationId', None)
    start = datetime.utcnow()
    
    try:
        if action == 'ping':
            response = {'status':'pong'}
            send_response(corr_id, response)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        if action == 'crawl_all':
            chain = req.get('chain','BHX').upper()
            # if chain == 'BHX':
            #     stores = crawl_bhx_all()
            # else:
            #     stores = crawl_wm_all()
            # result = {'status':'success','chain':chain,'stores':len(stores)}

        elif action == 'crawl_store':
            chain    = req.get('chain','BHX').upper()
            # store_id = req['storeId']
            # if chain == 'BHX':
            #     fetcher = BHXDataFetcher()
            #     asyncio.run(fetcher.init_token())
            #     # Giả sử demo.py có method crawl 1 store
            #     stores = asyncio.run(fetcher.fetch_store_by_id(store_id))
            #     asyncio.run(fetcher.close())
            # else:
            #     fetcher = WinMartFetcher()
            #     stores = fetcher.fetch_store_by_id(store_id)
            # result = {'status':'success','chain':chain,'storeId':store_id,'data':stores}

        else:
            result = {'status':'error','error':'unknown action'}

    except Exception as e:
        result = {'status':'error','error':str(e)}

    # Optionally gửi về RESPONSE_QUEUE
    if corr_id:
        send_response(corr_id, result)
    # ACK message
    ch.basic_ack(delivery_tag=method.delivery_tag)

def start_consumer():
    params = pika.URLParameters(RABBITMQ_URL)
    conn   = pika.BlockingConnection(params)
    ch     = conn.channel()
    ch.queue_declare(queue=REQUEST_QUEUE, durable=True)
    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=REQUEST_QUEUE, on_message_callback=on_request)
    print(' [*] Waiting for crawl requests. To exit press CTRL+C')
    ch.start_consuming()

if __name__ == '__main__':
    start_consumer()
