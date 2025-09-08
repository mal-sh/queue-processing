import os
import json
import redis
import boto3
import requests
import logging
from datetime import datetime
from urllib.parse import urlparse
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RedisConsumer:
    def __init__(self):
        # Load configuration from environment variables
        self.redis_host = os.getenv('REDIS_HOST', 'localhost')
        self.redis_port = int(os.getenv('REDIS_PORT', 6379))
        self.redis_db = int(os.getenv('REDIS_DB', 0))
        self.redis_password = os.getenv('REDIS_PASSWORD', None)
        self.queue_name = os.getenv('QUEUE_NAME', 'processing_queue')
        self.detail_view_api = os.getenv('DETAIL_VIEW_API', 'http://localhost:8000/api/detail')
        self.api_timeout = int(os.getenv('API_TIMEOUT', 45))  # Slightly more than 40 seconds
        
        # S3 configuration
        self.s3_access_key = os.getenv('S3_ACCESS_KEY')
        self.s3_secret_key = os.getenv('S3_SECRET_KEY')
        self.s3_endpoint = os.getenv('S3_ENDPOINT_URL')
        self.s3_bucket = os.getenv('S3_BUCKET_NAME')
        self.s3_region = os.getenv('S3_REGION', 'us-east-1')
        
        # Validate required environment variables
        self._validate_env_vars()
        
        # Initialize Redis connection
        self.redis_client = redis.Redis(
            host=self.redis_host,
            port=self.redis_port,
            db=self.redis_db,
            password=self.redis_password,
            decode_responses=True
        )
        
        # Initialize S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.s3_access_key,
            aws_secret_access_key=self.s3_secret_key,
            endpoint_url=self.s3_endpoint,
            region_name=self.s3_region
        )
        
        logger.info("Redis consumer initialized successfully")

    def _validate_env_vars(self):
        """Validate that all required environment variables are set"""
        required_vars = {
            'S3_ACCESS_KEY': self.s3_access_key,
            'S3_SECRET_KEY': self.s3_secret_key,
            'S3_ENDPOINT_URL': self.s3_endpoint,
            'S3_BUCKET_NAME': self.s3_bucket
        }
        
        missing_vars = [var for var, value in required_vars.items() if not value]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

    def is_valid_url(self, url: str) -> bool:
        """Validate if the provided string is a valid URL"""
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except:
            return False

    def call_detail_view_api(self, url: str) -> Optional[Dict[str, Any]]:
        """Call the detail view API with the provided URL"""
        payload = {"url": url}
        
        try:
            logger.info(f"Calling detail view API for URL: {url}")
            response = requests.post(
                self.detail_view_api,
                json=payload,
                timeout=self.api_timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            logger.error(f"API request timed out for URL: {url}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for URL {url}: {str(e)}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse API response for URL {url}: {str(e)}")
            return None

    def process_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a single message from Redis"""
        # Validate the URL
        if 'link' not in message or not self.is_valid_url(message['link']):
            logger.error(f"Invalid URL in message: {message.get('link', 'Missing')}")
            return None
        
        # Call the detail view API
        api_response = self.call_detail_view_api(message['link'])
        if not api_response:
            return None
        
        # Combine the original message with the API response
        combined_data = {**message, **api_response}
        return combined_data

    def store_in_s3(self, data: Dict[str, Any]) -> bool:
        """Store the combined data in S3"""
        # Create folder path based on today's date
        today = datetime.now().strftime('%Y-%m-%d')
        object_key = f"{today}/{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.json"
        
        try:
            # Convert data to JSON string
            json_data = json.dumps(data, ensure_ascii=False)
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=object_key,
                Body=json_data,
                ContentType='application/json'
            )
            
            logger.info(f"Successfully stored data in S3: {object_key}")
            return True
        except Exception as e:
            logger.error(f"Failed to store data in S3: {str(e)}")
            return False

    def run(self):
        """Main loop to process messages from Redis"""
        logger.info(f"Starting consumer for queue: {self.queue_name}")
        
        while True:
            try:
                # Use BLPOP to block until a message is available
                # This allows multiple instances to work together
                _, message_json = self.redis_client.blpop(self.queue_name, timeout=30)
                
                if message_json:
                    try:
                        message = json.loads(message_json)
                        logger.info(f"Processing message: {message.get('name', 'Unknown')}")
                        
                        # Process the message
                        combined_data = self.process_message(message)
                        
                        if combined_data:
                            # Store in S3
                            self.store_in_s3(combined_data)
                        else:
                            logger.warning(f"Failed to process message: {message.get('name', 'Unknown')}")
                    
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse message as JSON: {str(e)}")
                    except Exception as e:
                        logger.error(f"Unexpected error processing message: {str(e)}")
            
            except redis.exceptions.ConnectionError:
                logger.error("Redis connection error. Attempting to reconnect...")
                self._reconnect_redis()
            except Exception as e:
                logger.error(f"Unexpected error in main loop: {str(e)}")
                # Add a small delay to prevent tight loops on errors
                time.sleep(1)

    def _reconnect_redis(self):
        """Reconnect to Redis in case of connection issues"""
        try:
            self.redis_client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                db=self.redis_db,
                password=self.redis_password,
                decode_responses=True
            )
            # Test the connection
            self.redis_client.ping()
            logger.info("Redis reconnected successfully")
        except Exception as e:
            logger.error(f"Failed to reconnect to Redis: {str(e)}")
            time.sleep(5)  # Wait before retrying

if __name__ == "__main__":
    # Load environment variables from .env file if it exists
    try:
        from dotenv import load_dotenv
        load_dotenv()
        logger.info("Loaded environment variables from .env file")
    except ImportError:
        pass  # dotenv not installed
    
    try:
        consumer = RedisConsumer()
        consumer.run()
    except Exception as e:
        logger.error(f"Failed to initialize consumer: {str(e)}")
