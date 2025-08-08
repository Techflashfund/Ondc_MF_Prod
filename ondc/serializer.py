from rest_framework import serializers
from .models import Scheme

class SchemeSerializer(serializers.ModelSerializer):
    class Meta:
        model = Scheme
        fields = [
            "scheme_id",
            "name",
            "category_ids",
            "parent_item_id",
            "fulfillment_ids",
            "tags",
            "isin",
            "payload"
        ]
