# myapp/middleware.py
import logging

logger = logging.getLogger(__name__)

class LogHeadersMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        logger.info(f"Request Headers: {dict(request.headers)}")
        return self.get_response(request)
