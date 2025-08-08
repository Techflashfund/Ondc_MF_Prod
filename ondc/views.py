import json
import logging
import os
import uuid
from datetime import datetime
from threading import Thread

import requests
from django.core.exceptions import ObjectDoesNotExist
from django.http import JsonResponse
from django.shortcuts import get_object_or_404
from django.utils.dateparse import parse_datetime
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.db import transaction
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from .cryptic_utils import create_authorisation_header
from .models import (FullOnSearch,Scheme, Message, OnCancel, OnConfirm, OnInitSIP,
                     OnStatus, OnUpdate, PaymentSubmisssion, SelectSIP,
                     SubmissionID, Transaction,ONDCTransaction, MutualFundProvider, MutualFundScheme, 
    SchemePlan, FulfillmentOption, ONDCMutualFundService)
from .models import *
from .utils import (build_frequency, get_client_ip, push_observability_logs,
                    send_to_analytics)
from .serializer import SchemeSerializer

BAP_ID = "investment.flashfund.in"
BAP_URI = "https://investment.flashfund.in/ondc"


class ONDCSearchView(APIView):
    def post(self, request, *args, **kwargs):

        transaction_id = request.data.get("transaction_id")
        message_id = request.data.get("message_id")

        if not transaction_id or not message_id:
            transaction_id = str(uuid.uuid4())
            message_id = str(uuid.uuid4())

        timestamp = datetime.utcnow().isoformat(sep="T", timespec="milliseconds") + "Z"

        # Prepare payload
        payload = {
            "context": {
                "location": {"country": {"code": "IND"}, "city": {"code": "*"}},
                "domain": "ONDC:FIS14",
                "timestamp": timestamp,
                "bap_id": BAP_ID,
                "bap_uri": BAP_URI,
                "transaction_id": transaction_id,
                "message_id": message_id,
                "version": "2.0.0",
                "ttl": "PT10M",
                "action": "search",
            },
            "message": {
                "intent": {
                    "category": {"descriptor": {"code": "MUTUAL_FUNDS"}},
                    "fulfillment": {
                        "agent": {
                            "organization": {
                                "creds": [{"id": os.getenv("ARN"), "type": "ARN"}]
                            }
                        }
                    },
                    "tags": [
                        {
                            "display": False,
                            "descriptor": {
                                "name": "BAP Terms of Engagement",
                                "code": "BAP_TERMS",
                            },
                            "list": [
                                {
                                    "descriptor": {
                                        "name": "Static Terms (Transaction Level)",
                                        "code": "STATIC_TERMS",
                                    },
                                    "value": "https://buyerapp.com/legal/ondc:fis14/static_terms?v=0.1",
                                },
                                {
                                    "descriptor": {
                                        "name": "Offline Contract",
                                        "code": "OFFLINE_CONTRACT",
                                    },
                                    "value": "true",
                                },
                            ],
                        }
                    ],
                }
            },
        }
        # Store transaction and message
        transaction, _ = Transaction.objects.get_or_create(
            transaction_id=transaction_id
        )
        message, created = Message.objects.get_or_create(
            message_id=message_id,
            defaults={
                "transaction": transaction,
                "action": "search",
                "timestamp": parse_datetime(timestamp),
                "payload": payload,
            },
        )
        if not created:
            print(f"Message with ID {message_id} already exists. Skipping insert.")

        # Send to gateway
        request_body_str = json.dumps(payload, separators=(",", ":"))
        auth_header = create_authorisation_header(request_body=request_body_str)

        headers = {
            "Content-Type": "application/json",
            "Authorization": auth_header,
            "X-Gateway-Authorization": os.getenv("SIGNED_UNIQUE_REQ_ID", ""),
            "X-Gateway-Subscriber-Id": os.getenv("SUBSCRIBER_ID"),
        }

        response = requests.post(
            "https://prod.gateway.ondc.org/search",
            data=request_body_str,
            headers=headers,
        )

        try:
            send_to_analytics(schema_type="search", req_body=payload)
        except Exception as e:
            logger.error(f"Observability logging failed: {str(e)}", exc_info=True)

        try:
            resp_data = response.json()
        except Exception:
            resp_data = response.text  # Fallback to raw string (e.g. HTML or 404)

        return Response(
            {"status_code": response.status_code, "response": resp_data},
            status=response.status_code,
        )


logger = logging.getLogger(__name__)


class OnSearchView(APIView):
    def post(self, request, *args, **kwargs):
        try:
            data = request.data
            logger.info("Received on_search callback:\n%s", json.dumps(data, indent=2))
            print("Received on_search callback:\n", json.dumps(data, indent=2))

            context = data.get("context", {})
            message_id = context.get("message_id")
            transaction_id = context.get("transaction_id")
            timestamp_str = context.get("timestamp")

            # Validate required fields
            if not all([message_id, transaction_id, timestamp_str]):
                return Response(
                    {
                        "message": {
                            "ack": {"status": "NACK"},
                        }
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            # Parse timestamp
            timestamp = parse_datetime(timestamp_str)
            if not timestamp:
                return Response(
                    {
                        "message": {"ack": {"status": "NACK"}},
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            # Get related transaction
            try:
                txn = Transaction.objects.get(transaction_id=transaction_id)
            except Transaction.DoesNotExist:
                return Response(
                    {"message": {"ack": {"status": "NACK"}}},
                    status=status.HTTP_404_NOT_FOUND,
                )

            # try:
            #     isin = data["message"]["catalog"]["providers"][0]["items"][1]["tags"][
            #         1
            #     ]["list"][0]["value"]
            # except (KeyError, IndexError, TypeError):
            #     isin = None

            # Save to database
            with transaction.atomic():
                # 1. Create ONDCTransaction record
                txn, created = ONDCTransaction.objects.get_or_create(
                    transaction_id=transaction_id,
                    defaults={
                        "message_id": message_id,
                        "bap_id": context.get("bap_id"),
                        "bap_uri": context.get("bap_uri"),
                        "bpp_id": context.get("bpp_id"),
                        "bpp_uri": context.get("bpp_uri"),
                        "domain": context.get("domain"),
                        "version": context.get("version"),
                        "action": context.get("action"),
                        "country_code": context.get("location", {}).get("country", {}).get("code", "IND"),
                        "city_code": context.get("location", {}).get("city", {}).get("code", "*"),
                        "timestamp": timestamp,
                        "ttl": context.get("ttl"),
                    }
                )
                
                catalog = data.get("message", {}).get("catalog", {})
                providers_data = catalog.get("providers", [])
                categories_data = catalog.get("categories", [])
                tags_data = catalog.get("tags", [])

                # Optional: Save BPP terms if tags are present (you can expand this)
                # if tags_data:
                #    process_bpp_terms(tags_data, txn)

                # 2. Create or update providers and related data
                for provider_data in providers_data:
                    provider, _ = MutualFundProvider.objects.update_or_create(
                        provider_id=provider_data["id"],
                        bpp_id=context.get("bpp_id"),
                        defaults={"name": provider_data["descriptor"]["name"], "is_active": True},
                    )

                    # 3. Process categories for this provider if present
                    provider_categories = [c for c in categories_data if c.get("provider_id") == provider_data["id"]]
                    categories_map = {}
                    for cat_data in provider_categories:
                        parent = None
                        if "parent_category_id" in cat_data:
                            parent = SchemeCategory.objects.filter(category_id=cat_data["parent_category_id"], provider=provider).first()
                        category, _ = SchemeCategory.objects.update_or_create(
                            category_id=cat_data["id"],
                            provider=provider,
                            defaults={
                                "name": cat_data["descriptor"]["name"],
                                "code": cat_data["descriptor"]["code"],
                                "parent_category": parent,
                                "level": (parent.level + 1) if parent else 0,
                            },
                        )
                        categories_map[cat_data["id"]] = category

                    # 4 & 5. Process schemes and plans
                    items = provider_data.get("items", [])
                    schemes = [i for i in items if i["descriptor"]["code"] == "SCHEME"]
                    plans = [i for i in items if i["descriptor"]["code"] == "SCHEME_PLAN"]
                    fulfillments = {f["id"]: f for f in provider_data.get("fulfillments", [])}

                    for scheme_data in schemes:
                        scheme = MutualFundScheme.objects.create(
                            scheme_id=scheme_data["id"],
                            provider=provider,
                            transaction=txn,
                            name=scheme_data["descriptor"]["name"],
                            code=scheme_data["descriptor"]["code"],
                            # add other fields after parsing tags as needed
                        )
                        # Add categories to scheme
                        for cat_id in scheme_data.get("category_ids", []):
                            if cat_id in categories_map:
                                scheme.categories.add(categories_map[cat_id])

                        # Create scheme plans linked to this scheme
                        scheme_plans = [p for p in plans if p.get("parent_item_id") == scheme_data["id"]]
                        for plan_data in scheme_plans:
                            plan = SchemePlan.objects.create(
                                plan_id=plan_data["id"],
                                scheme=scheme,
                                name=plan_data["descriptor"]["name"],
                                code=plan_data["descriptor"]["code"],
                                # add other plan-specific fields
                            )

                            # 6. Create fulfillment options linked to plan
                            for fid in plan_data.get("fulfillment_ids", []):
                                fulfillment_data = fulfillments.get(fid)
                                if fulfillment_data:
                                    FulfillmentOption.objects.create(
                                        fulfillment_id=fid,
                                        plan=plan,
                                        fulfillment_type=fulfillment_data["type"],
                                        # add other fulfillment fields (thresholds, etc.) if needed
                                    )
                      

            try:
                send_to_analytics(schema_type="on_search", req_body=data)
            except Exception as e:
                logger.error(f"Observability logging failed: {str(e)}", exc_info=True)

        except Exception as e:
            logger.error("Failed to process on_search data: %s", str(e), exc_info=True)
            return Response(
                {
                    "message": {
                        "ack": {"status": "NACK"},
                    }
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        # Success response
        return Response(
            {"message": {"ack": {"status": "ACK"}}}, status=status.HTTP_200_OK
        )
    
    @staticmethod
    def extract_isin_from_tags(tags):
        if not tags:
            return None
        for tag in tags:
            if tag.get("descriptor", {}).get("code") == "PLAN_IDENTIFIERS":
                for item in tag.get("list", []):
                    if item.get("descriptor", {}).get("code") == "ISIN":
                        return item.get("value")
        return None

class SchemeByISINView(APIView):
    def get(self, request, *args, **kwargs):
        isin = request.query_params.get("isin")
        if not isin:
            return Response(
                {"error": "Missing required query parameter: isin"},
                status=status.HTTP_400_BAD_REQUEST
            )

        scheme = get_object_or_404(Scheme, isin=isin)
        serializer = SchemeSerializer(scheme)
        return Response(serializer.data, status=status.HTTP_200_OK)



class OnSearchDataView(APIView):
    def post(self, request, *args, **kwargs):
        transaction_id = request.data.get("transaction_id")

        if not transaction_id:
            return Response(
                {"error": "Missing transaction_id"}, status=status.HTTP_400_BAD_REQUEST
            )

        try:
            transaction = Transaction.objects.get(transaction_id=transaction_id)
            search_entries = FullOnSearch.objects.filter(transaction=transaction)

            response_data = []
            for entry in search_entries:
                response_data.append(
                    {
                        "message_id": entry.message_id,
                        "timestamp": entry.timestamp,
                        "payload": entry.payload,
                    }
                )

            return Response(response_data, status=status.HTTP_200_OK)

        except ObjectDoesNotExist:
            return Response(
                {"error": "Transaction not found"}, status=status.HTTP_404_NOT_FOUND
            )
        except Exception as e:
            logger.error("Failed to fetch FullOnSearch data: %s", str(e))
            return Response(
                {"error": "Server error"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


# SIP Creation Without KYC


class SIPCreationView(APIView):

    def post(self, request, *args, **kwargs):
        transaction_id = request.data.get("transaction_id")
        message_id = request.data.get("message_id")
        bpp_id = request.data.get("bpp_id")
        bpp_uri = request.data.get("bpp_uri")
        preferred_type = "SIP"
        amount = request.data.get("amount")
        pan = request.data.get("pan")
        frequency = request.data.get("frequency")
        repeat = request.data.get("repeat")
        date = request.data.get("date")

        if not all([transaction_id, bpp_id, bpp_uri]):
            return Response(
                {"error": "Missing transaction_id, bpp_id, or bpp_uri"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        obj = get_object_or_404(
            FullOnSearch,
            payload__context__bpp_id=bpp_id,
            payload__context__bpp_uri=bpp_uri,
            transaction__transaction_id=transaction_id,
        )

        if not message_id:
            message_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat(sep="T", timespec="milliseconds") + "Z"

        # Get the first provider and item
        provider = obj.payload["message"]["catalog"]["providers"][0]
        catalog = obj.payload["message"]["catalog"]
        matching_fulfillment = next(
            (f for f in provider["fulfillments"] if f.get("type") == preferred_type),
            None,
        )

        if not matching_fulfillment:
            return Response(
                {"error": f"No fulfillment with type '{preferred_type}' found."},
                status=status.HTTP_404_NOT_FOUND,
            )

        payload = {
            "context": {
                "location": {"country": {"code": "IND"}, "city": {"code": "*"}},
                "domain": "ONDC:FIS14",
                "timestamp": timestamp,
                "bap_id": BAP_ID,
                "bap_uri": BAP_URI,
                "transaction_id": transaction_id,
                "message_id": message_id,
                "version": "2.0.0",
                "ttl": "PT10M",
                "bpp_id": bpp_id,
                "bpp_uri": bpp_uri,
                "action": "select",
            },
            "message": {
                "order": {
                    "provider": {"id": provider["id"]},
                    "items": [
                        {
                            "id": provider["items"][0]["id"],
                            "quantity": {
                                "selected": {
                                    "measure": {"value": amount, "unit": "INR"}
                                }
                            },
                        }
                    ],
                    "fulfillments": [
                        {
                            "id": matching_fulfillment["id"],
                            "type": matching_fulfillment["type"],
                            "customer": {"person": {"id": "pan:" + pan}},
                            "agent": {
                                "person": {"id": os.getenv("EUIN")},
                                "organization": {
                                    "creds": [
                                        {"id": os.getenv("ARN"), "type": "ARN"},
                                    ]
                                },
                            },
                            "stops": [
                                {
                                    "time": {
                                        "schedule": {
                                            "frequency": build_frequency(
                                                frequency=frequency,
                                                repeat=repeat,
                                                day_number=date,
                                            )
                                        }
                                    }
                                }
                            ],
                        }
                    ],
                    "tags": [
                        {
                            "display": False,
                            "descriptor": {
                                "name": "BAP Terms of Engagement",
                                "code": "BAP_TERMS",
                            },
                            "list": [
                                {
                                    "descriptor": {
                                        "name": "Static Terms (Transaction Level)",
                                        "code": "STATIC_TERMS",
                                    },
                                    "value": "https://buyerapp.com/legal/ondc:fis14/static_terms?v=0.1",
                                },
                                {
                                    "descriptor": {
                                        "name": "Offline Contract",
                                        "code": "OFFLINE_CONTRACT",
                                    },
                                    "value": "true",
                                },
                            ],
                        }
                    ],
                }
            },
        }

        transaction = Transaction.objects.get(transaction_id=transaction_id)
        Message.objects.create(
            transaction=transaction,
            message_id=message_id,
            action="select",
            timestamp=parse_datetime(timestamp),
            payload=payload,
        )

        # Send to gateway
        request_body_str = json.dumps(payload, separators=(",", ":"))
        auth_header = create_authorisation_header(request_body=request_body_str)

        headers = {
            "Content-Type": "application/json",
            "Authorization": auth_header,
            "X-Gateway-Authorization": os.getenv("SIGNED_UNIQUE_REQ_ID", ""),
            "X-Gateway-Subscriber-Id": os.getenv("SUBSCRIBER_ID"),
        }

        response = requests.post(
            f"{bpp_uri}/select", data=request_body_str, headers=headers
        )

        try:
            send_to_analytics(schema_type="select", req_body=payload)
        except Exception as e:
            logger.error(f"Observability logging failed: {str(e)}", exc_info=True)

        return Response(
            {
                "status_code": response.status_code,
                "response": response.json() if response.content else {},
            },
            status=status.HTTP_200_OK,
        )


logger = logging.getLogger(__name__)


class OnSelectSIPView(APIView):
    def post(self, request, *args, **kwargs):
        try:
            data = request.data
            logger.info("Received on_select payload: %s", data)
            print("Received on_select payload:", json.dumps(data, indent=2))

            context = data.get("context", {})
            message_id = context.get("message_id")
            transaction_id = context.get("transaction_id")
            timestamp_str = context.get("timestamp")
            action = context.get("action")

            # Validate context fields
            if not all([message_id, transaction_id, timestamp_str, action]):
                return Response(
                    {
                        "message": {"ack": {"status": "NACK"}},
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            if action != "on_select":
                return Response(
                    {
                        "message": {"ack": {"status": "NACK"}},
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            # Validate timestamp
            timestamp = parse_datetime(timestamp_str)
            if not timestamp:
                return Response(
                    {
                        "message": {"ack": {"status": "NACK"}},
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            # Validate transaction
            try:
                transaction = Transaction.objects.get(transaction_id=transaction_id)
            except Transaction.DoesNotExist:
                logger.warning("Transaction not found: %s", transaction_id)
                return Response(
                    {
                        "message": {"ack": {"status": "NACK"}},
                    },
                    status=status.HTTP_404_NOT_FOUND,
                )

            # Save data
            SelectSIP.objects.create(
                transaction=transaction,
                message_id=message_id,
                payload=data,
                timestamp=timestamp,
            )
            try:
                send_to_analytics(schema_type="on_select", req_body=data)
            except Exception as e:
                logger.error(f"Observability logging failed: {str(e)}", exc_info=True)
        except Exception as e:
            logger.error("Failed to process on_select: %s", str(e), exc_info=True)
            return Response(
                {
                    "message": {
                        "ack": {"status": "NACK"},
                    }
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        # Success response
        return Response(
            {"message": {"ack": {"status": "ACK"}}}, status=status.HTTP_200_OK
        )


# form Submission


class FormSubmisssion(APIView):
    def post(self, request, *args, **kwargs):
        transaction_id = request.data.get("transaction_id")
        bpp_id = request.data.get("bpp_id")
        bpp_uri = request.data.get("bpp_uri")
        form_data = request.data.get("form_data")
        message_id = request.data.get("message_id")
        if not all([transaction_id, bpp_id, bpp_uri]):
            return Response(
                {"error": "Missing transaction_id, bpp_id, or bpp_uri"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        obj = get_object_or_404(
            SelectSIP,
            payload__context__bpp_id=bpp_id,
            payload__context__bpp_uri=bpp_uri,
            transaction__transaction_id=transaction_id,
        )
        if not message_id:
            message_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat(sep="T", timespec="milliseconds") + "Z"

        try:
            provider = obj.payload["message"]["order"]["provider"]
            item = obj.payload["message"]["order"]["items"]
            fulfillments = obj.payload["message"]["order"]["fulfillments"]
            xinput = obj.payload["message"]["order"]["xinput"]
            url = obj.payload["message"]["order"]["xinput"]["form"]["url"]
        except (KeyError, TypeError):
            return Response(
                {"error": "Form URL not found in payload"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        # user_kyc_data = {
        #     "pan": "ABCDE1234F",
        #     "dob": "1990-01-01",
        #     "email": "user@example.com",
        #     "name": "Ravi Kumar",
        #     "gender":"Male",
        #     "marital_status":"Married",
        #     "occupation":"Salaried",
        #     "source_of_wealth":"Business",
        #     "income_range":"1L to 5L",
        #     "cob":"India",
        #     "pob":"Kochi",
        #     "political_exposure":"no_exposure",
        #     "india_tax_residency_status":"resident",
        #     "mode_of_holding":"single",
        #     "ca_line":"hfjfk jifl jffj",

        # }
        # if form_data:
        #     user_kyc_data=form_data
        try:
            res = requests.post(url, json=form_data)
            if res.status_code == 200:
                resp_json = res.json()
                submission_id = resp_json["submission_id"]
                if not submission_id:
                    return Response(
                        {"error": "submission id missing"},
                        status=status.HTTP_400_BAD_REQUEST,
                    )

                SubmissionID.objects.create(
                    transaction=obj.transaction,
                    submission_id=submission_id,
                    message_id=message_id,
                    timestamp=timestamp,
                )
                payload = {
                    "context": {
                        "location": {"country": {"code": "IND"}, "city": {"code": "*"}},
                        "domain": "ONDC:FIS14",
                        "timestamp": timestamp,
                        "bap_id": BAP_ID,
                        "bap_uri": BAP_URI,
                        "transaction_id": transaction_id,
                        "message_id": message_id,
                        "version": "2.0.0",
                        "ttl": "PT10M",
                        "bpp_id": bpp_id,
                        "bpp_uri": bpp_uri,
                        "action": "select",
                    },
                    "message": {
                        "order": {
                            "provider": {"id": provider["id"]},
                            "items": [
                                {
                                    "id": item[0]["id"],
                                    "quantity": {
                                        "selected": {
                                            "measure": {
                                                "value": item[0]["quantity"][
                                                    "selected"
                                                ]["measure"]["value"],
                                                "unit": item[0]["quantity"]["selected"][
                                                    "measure"
                                                ]["unit"],
                                            }
                                        }
                                    },
                                    "fulfillment_ids": [item[0]["fulfillment_ids"][0]],
                                }
                            ],
                            "fulfillments": [
                                {
                                    "id": fulfillments[0]["id"],
                                    "type": fulfillments[0]["type"],
                                    "customer": {
                                        "person": {
                                            "id": fulfillments[0]["customer"]["person"][
                                                "id"
                                            ]
                                        }
                                    },
                                    "agent": {
                                        "person": {
                                            "id": fulfillments[0]["agent"]["person"][
                                                "id"
                                            ]
                                        },
                                        "organization": {
                                            "creds": [
                                                {
                                                    "id": fulfillments[0]["agent"][
                                                        "organization"
                                                    ]["creds"][0]["id"],
                                                    "type": fulfillments[0]["agent"][
                                                        "organization"
                                                    ]["creds"][0]["type"],
                                                },
                                            ]
                                        },
                                    },
                                    "stops": [
                                        {
                                            "time": {
                                                "schedule": {
                                                    "frequency": fulfillments[0][
                                                        "stops"
                                                    ][0]["time"]["schedule"][
                                                        "frequency"
                                                    ]
                                                }
                                            }
                                        }
                                    ],
                                }
                            ],
                            "xinput": {
                                "form": {"id": xinput["form"]["id"]},
                                "form_response": {"submission_id": submission_id},
                            },
                            "tags": [
                                {
                                    "display": False,
                                    "descriptor": {
                                        "name": "BAP Terms of Engagement",
                                        "code": "BAP_TERMS",
                                    },
                                    "list": [
                                        {
                                            "descriptor": {
                                                "name": "Static Terms (Transaction Level)",
                                                "code": "STATIC_TERMS",
                                            },
                                            "value": "https://buyerapp.com/legal/ondc:fis14/static_terms?v=0.1",
                                        },
                                        {
                                            "descriptor": {
                                                "name": "Offline Contract",
                                                "code": "OFFLINE_CONTRACT",
                                            },
                                            "value": "true",
                                        },
                                    ],
                                }
                            ],
                        }
                    },
                }
                # Send to gateway
                request_body_str = json.dumps(payload, separators=(",", ":"))
                auth_header = create_authorisation_header(request_body=request_body_str)

                headers = {
                    "Content-Type": "application/json",
                    "Authorization": auth_header,
                    "X-Gateway-Authorization": os.getenv("SIGNED_UNIQUE_REQ_ID", ""),
                    "X-Gateway-Subscriber-Id": os.getenv("SUBSCRIBER_ID"),
                }

                response = requests.post(
                    f"{bpp_uri}/select", data=request_body_str, headers=headers
                )
                try:
                    send_to_analytics(schema_type="select", req_body=payload)
                except Exception as e:
                    logger.error(
                        f"Observability logging failed: {str(e)}", exc_info=True
                    )
                return Response(
                    {
                        "status_code": response.status_code,
                        "response": response.json() if response.content else {},
                    },
                    status=status.HTTP_200_OK,
                )
            # else:
            #     return Response(
            #         {"error": f"Form upload failed with status {res.status_code}"},
            #         status=status.HTTP_400_BAD_REQUEST
            #     )

            error_response = {
                "error": "Form upload failed",
                "status_code": res.status_code,
                "ondc_response": res.json() if res.content else None,
                "request_url": url,
                "request_payload": form_data,  # Be careful with sensitive data
            }
            return Response(error_response, status=status.HTTP_400_BAD_REQUEST)
        except requests.exceptions.RequestException as e:
            return Response(
                {"error": f"Form upload failed: {str(e)}"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        except Exception as e:
            return Response(
                {"error": f"Unexpected error: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


class INIT(APIView):

    def post(self, request, *args, **kwargs):
        transaction_id = request.data.get("transaction_id")
        bpp_id = request.data.get("bpp_id")
        bpp_uri = request.data.get("bpp_uri")
        message_id_select = request.data.get("message_id_select")
        phone = request.data.get("phone", "12345678")
        ifsc = request.data.get("ifsc", "HDFC00014567")
        account_number = request.data.get("account_number", "1234578")
        name = request.data.get("name", "John")
        acs_type = request.data.get("acs_type", "Savings")
        payment_mode = request.data.get("payment_mode")
        message_id = request.data.get("message_id")
        if not all([transaction_id, bpp_id, bpp_uri, message_id_select]):
            return Response(
                {"error": "Required all Fields"}, status=status.HTTP_400_BAD_REQUEST
            )

        obj = get_object_or_404(
            SelectSIP,
            payload__context__bpp_id=bpp_id,
            payload__context__bpp_uri=bpp_uri,
            transaction__transaction_id=transaction_id,
            payload__context__message_id=message_id_select,
        )
        if not message_id:
            message_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat(sep="T", timespec="milliseconds") + "Z"

        try:
            provider = obj.payload["message"]["order"]["provider"]
            item = obj.payload["message"]["order"]["items"]
            fulfillments = obj.payload["message"]["order"]["fulfillments"]
            payments = obj.payload["message"]["order"]["payments"]
        except (KeyError, TypeError) as e:
            return Response(
                {"error": f"Missing key in payload: {e}"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        payload = {
            "context": {
                "location": {"country": {"code": "IND"}, "city": {"code": "*"}},
                "domain": "ONDC:FIS14",
                "timestamp": timestamp,
                "bap_id": BAP_ID,
                "bap_uri": BAP_URI,
                "transaction_id": transaction_id,
                "message_id": message_id,
                "version": "2.0.0",
                "ttl": "PT10M",
                "bpp_id": bpp_id,
                "bpp_uri": bpp_uri,
                "action": "init",
            },
            "message": {
                "order": {
                    "provider": {"id": provider["id"]},
                    "items": [
                        {
                            "id": item[0]["id"],
                            "quantity": {
                                "selected": {
                                    "measure": {
                                        "value": item[0]["quantity"]["selected"][
                                            "measure"
                                        ]["value"],
                                        "unit": "INR",
                                    }
                                }
                            },
                            "fulfillment_ids": [item[0]["fulfillment_ids"][0]],
                        }
                    ],
                    "fulfillments": [
                        {
                            "id": fulfillments[0]["id"],
                            "type": fulfillments[0]["type"],
                            "customer": {
                                "person": {
                                    "id": fulfillments[0]["customer"]["person"]["id"],
                                    "creds": [
                                        {
                                            "id": get_client_ip(request),
                                            "type": "IP_ADDRESS",
                                        }
                                    ],
                                },
                                "contact": {"phone": phone},
                            },
                            "agent": {
                                "person": {
                                    "id": fulfillments[0]["agent"]["person"]["id"]
                                },
                                "organization": {
                                    "creds": [
                                        {
                                            "id": fulfillments[0]["agent"][
                                                "organization"
                                            ]["creds"][0]["id"],
                                            "type": fulfillments[0]["agent"][
                                                "organization"
                                            ]["creds"][0]["type"],
                                        },
                                    ]
                                },
                            },
                            "stops": [
                                {
                                    "time": {
                                        "schedule": {
                                            "frequency": fulfillments[0]["stops"][0][
                                                "time"
                                            ]["schedule"]["frequency"]
                                        }
                                    }
                                }
                            ],
                        }
                    ],
                    "payments": [
                        {
                            "collected_by": payments[0]["collected_by"],
                            "params": {
                                "amount": item[0]["quantity"]["selected"]["measure"][
                                    "value"
                                ],
                                "currency": "INR",
                                "source_bank_code": ifsc,
                                "source_bank_account_number": account_number,
                                "source_bank_account_name": name,
                            },
                            "type": payments[0]["type"],
                            "tags": [
                                {
                                    "descriptor": {
                                        "name": "Payment Method",
                                        "code": "PAYMENT_METHOD",
                                    },
                                    "list": [
                                        {
                                            "descriptor": {"code": "MODE"},
                                            "value": payment_mode,
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                    "tags": [
                        {
                            "display": False,
                            "descriptor": {
                                "name": "BAP Terms of Engagement",
                                "code": "BAP_TERMS",
                            },
                            "list": [
                                {
                                    "descriptor": {
                                        "name": "Static Terms (Transaction Level)",
                                        "code": "STATIC_TERMS",
                                    },
                                    "value": "https://buyerapp.com/legal/ondc:fis14/static_terms?v=0.1",
                                },
                                {
                                    "descriptor": {
                                        "name": "Offline Contract",
                                        "code": "OFFLINE_CONTRACT",
                                    },
                                    "value": "true",
                                },
                            ],
                        }
                    ],
                }
            },
        }
        # Send to gateway
        request_body_str = json.dumps(payload, separators=(",", ":"))
        auth_header = create_authorisation_header(request_body=request_body_str)

        headers = {
            "Content-Type": "application/json",
            "Authorization": auth_header,
            "X-Gateway-Authorization": os.getenv("SIGNED_UNIQUE_REQ_ID", ""),
            "X-Gateway-Subscriber-Id": os.getenv("SUBSCRIBER_ID"),
        }

        response = requests.post(
            f"{bpp_uri}/init", data=request_body_str, headers=headers
        )
        try:
            send_to_analytics(schema_type="init", req_body=payload)
        except Exception as e:
            logger.error(f"Observability logging failed: {str(e)}", exc_info=True)
        return Response(
            {
                "status_code": response.status_code,
                "response": response.json() if response.content else {},
            },
            status=status.HTTP_200_OK,
        )


class ONINIT(APIView):
    def post(self, request, *args, **kwargs):
        try:
            data = request.data
            logger.info("Received on_init payload: %s", data)
            print("Received on_init payload:", json.dumps(data, indent=2))

            context = data.get("context", {})
            message_id = context.get("message_id")
            transaction_id = context.get("transaction_id")
            timestamp_str = context.get("timestamp")
            action = context.get("action")

            # Validate context fields
            if not all([message_id, transaction_id, timestamp_str, action]):
                return Response(
                    {
                        "message": {"ack": {"status": "NACK"}},
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            if action != "on_init":
                return Response(
                    {
                        "message": {"ack": {"status": "NACK"}},
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            # Validate timestamp
            timestamp = parse_datetime(timestamp_str)
            if not timestamp:
                return Response(
                    {
                        "context": context,
                        "message": {
                            "ack": {"status": "NACK"},
                            "error": {
                                "type": "TIMESTAMP-ERROR",
                                "message": "Invalid timestamp format",
                            },
                        },
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            # Validate transaction
            try:
                transaction = Transaction.objects.get(transaction_id=transaction_id)
            except Transaction.DoesNotExist:
                logger.warning("Transaction not found: %s", transaction_id)
                return Response(
                    {
                        "message": {"ack": {"status": "NACK"}},
                    },
                    status=status.HTTP_404_NOT_FOUND,
                )

            # Save to database
            OnInitSIP.objects.create(
                transaction=transaction,
                message_id=message_id,
                payload=data,
                timestamp=timestamp,
            )

            try:
                send_to_analytics(schema_type="on_init", req_body=data)
            except Exception as e:
                logger.error(f"Observability logging failed: {str(e)}", exc_info=True)

        except Exception as e:
            logger.error("Failed to process on_init data: %s", str(e))
            return Response(
                {
                    "context": context if "context" in locals() else {},
                    "message": {"ack": {"status": "NACK"}},
                    "error": {"type": "SERVER-ERROR", "message": str(e)},
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        # Success response
        return Response(
            {"message": {"ack": {"status": "ACK"}}}, status=status.HTTP_200_OK
        )


class ConfirmSIP(APIView):
    def post(self, request, *args, **kwargs):
        transaction_id = request.data.get("transaction_id")
        bpp_id = request.data.get("bpp_id")
        bpp_uri = request.data.get("bpp_uri")
        message_id_init = request.data.get("message_id_init")
        message_id = request.data.get("message_id")

        if not all([transaction_id, bpp_id, bpp_uri, message_id_init]):
            return Response(
                {"error": "Required all Fields"}, status=status.HTTP_400_BAD_REQUEST
            )

        obj = get_object_or_404(
            OnInitSIP,
            payload__context__bpp_id=bpp_id,
            payload__context__bpp_uri=bpp_uri,
            transaction__transaction_id=transaction_id,
            payload__context__message_id=message_id_init,
        )
        if not message_id:
            message_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat(sep="T", timespec="milliseconds") + "Z"

        try:
            id = obj.payload["message"]["order"]["id"]
            provider = obj.payload["message"]["order"]["provider"]
            item = obj.payload["message"]["order"]["items"]
            fulfillments = obj.payload["message"]["order"]["fulfillments"]
            payments = obj.payload["message"]["order"]["payments"]
        except (KeyError, TypeError) as e:
            return Response(
                {"error": f"Missing key in payload: {e}"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        try:
            payment_method = payments[0]["tags"][0]["list"][0]["value"]
        except (IndexError, KeyError):
            return Response(
                {"error": "Missing payment method in payment tags"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Determine payment type based on payment method
        if payment_method == "MANDATE_REGISTRATION":
            payment_type = "PRE_FULFILLMENT"
        elif payment_method == "UPI_ON_DELIVERY":
            payment_type = "ON_FULFILLMENT"
        else:
            payment_type = "POST_FULFILLMENT"

        payload = {
            "context": {
                "location": {"country": {"code": "IND"}, "city": {"code": "*"}},
                "domain": "ONDC:FIS14",
                "timestamp": timestamp,
                "bap_id": BAP_ID,
                "bap_uri": BAP_URI,
                "transaction_id": transaction_id,
                "message_id": message_id,
                "version": "2.0.0",
                "ttl": "PT10M",
                "bpp_id": bpp_id,
                "bpp_uri": bpp_uri,
                "action": "confirm",
            },
            "message": {
                "order": {
                    "id": id,
                    "provider": {"id": provider["id"]},
                    "items": [
                        {
                            "id": item[0]["id"],
                            "quantity": {
                                "selected": {
                                    "measure": {
                                        "value": item[0]["quantity"]["selected"][
                                            "measure"
                                        ]["value"],
                                        "unit": "INR",
                                    }
                                }
                            },
                            "fulfillment_ids": [item[0]["fulfillment_ids"][0]],
                            "payment_ids": [item[0]["payment_ids"][0]],
                        }
                    ],
                    "fulfillments": [
                        {
                            "id": fulfillments[0]["id"],
                            "type": fulfillments[0]["type"],
                            "customer": {
                                "person": {
                                    "id": fulfillments[0]["customer"]["person"]["id"],
                                    "creds": [
                                        {
                                            "id": fulfillments[0]["customer"]["person"][
                                                "creds"
                                            ][0]["id"],
                                            "type": "IP_ADDRESS",
                                        }
                                    ],
                                },
                                "contact": {
                                    "phone": fulfillments[0]["customer"]["contact"][
                                        "phone"
                                    ]
                                },
                            },
                            "agent": {
                                "person": {
                                    "id": fulfillments[0]["agent"]["person"]["id"]
                                },
                                "organization": {
                                    "creds": [
                                        {
                                            "id": fulfillments[0]["agent"][
                                                "organization"
                                            ]["creds"][0]["id"],
                                            "type": "ARN",
                                        },
                                    ]
                                },
                            },
                            "stops": [
                                {
                                    "time": {
                                        "schedule": {
                                            "frequency": fulfillments[0]["stops"][0][
                                                "time"
                                            ]["schedule"]["frequency"]
                                        }
                                    }
                                }
                            ],
                        }
                    ],
                    "payments": [
                        {
                            "id": payments[0]["id"],
                            "collected_by": payments[0]["collected_by"],
                            "status": payments[0]["status"],
                            "params": {
                                "amount": payments[0]["params"]["amount"],
                                "currency": "INR",
                                "source_bank_code": payments[0]["params"][
                                    "source_bank_code"
                                ],
                                "source_bank_account_number": payments[0]["params"][
                                    "source_bank_account_number"
                                ],
                                "source_bank_account_name": payments[0]["params"][
                                    "source_bank_account_name"
                                ],
                                "transaction_id": payments[0]["id"],
                            },
                            "type": payment_type,
                            "tags": [
                                {
                                    "descriptor": {
                                        "name": payments[0]["tags"][0]["descriptor"][
                                            "name"
                                        ],
                                        "code": payments[0]["tags"][0]["descriptor"][
                                            "code"
                                        ],
                                    },
                                    "list": [
                                        {
                                            "descriptor": {
                                                "code": payments[0]["tags"][0]["list"][
                                                    0
                                                ]["descriptor"]["code"]
                                            },
                                            "value": payments[0]["tags"][0]["list"][0][
                                                "value"
                                            ],
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                    "tags": [
                        {
                            "display": False,
                            "descriptor": {
                                "name": "BAP Terms of Engagement",
                                "code": "BAP_TERMS",
                            },
                            "list": [
                                {
                                    "descriptor": {
                                        "name": "Static Terms (Transaction Level)",
                                        "code": "STATIC_TERMS",
                                    },
                                    "value": "https://buyerapp.com/legal/ondc:fis14/static_terms?v=0.1",
                                },
                                {
                                    "descriptor": {
                                        "name": "Offline Contract",
                                        "code": "OFFLINE_CONTRACT",
                                    },
                                    "value": "true",
                                },
                            ],
                        },
                        {
                            "display": False,
                            "descriptor": {
                                "name": "BPP Terms of Engagement",
                                "code": "BPP_TERMS",
                            },
                            "list": [
                                {
                                    "descriptor": {
                                        "name": "Static Terms (Transaction Level)",
                                        "code": "STATIC_TERMS",
                                    },
                                    "value": "https://sellerapp.com/legal/ondc:fis14/static_terms?v=0.1",
                                },
                                {
                                    "descriptor": {
                                        "name": "Offline Contract",
                                        "code": "OFFLINE_CONTRACT",
                                    },
                                    "value": "true",
                                },
                            ],
                        },
                    ],
                }
            },
        }
        # Send to gateway

        request_body_str = json.dumps(payload, separators=(",", ":"))
        auth_header = create_authorisation_header(request_body=request_body_str)

        headers = {
            "Content-Type": "application/json",
            "Authorization": auth_header,
            "X-Gateway-Authorization": os.getenv("SIGNED_UNIQUE_REQ_ID", ""),
            "X-Gateway-Subscriber-Id": os.getenv("SUBSCRIBER_ID"),
        }

        response = requests.post(
            f"{bpp_uri}/confirm", data=request_body_str, headers=headers
        )
        try:
            send_to_analytics(schema_type="confirm", req_body=payload)
        except Exception as e:
            logger.error(f"Observability logging failed: {str(e)}", exc_info=True)
        return Response(
            {
                "status_code": response.status_code,
                "response": response.json() if response.content else {},
            },
            status=status.HTTP_200_OK,
        )


class OnConfirmSIP(APIView):
    def post(self, request, *args, **kwargs):
        try:
            data = request.data
            logger.info("Received on_confirm payload: %s", data)
            print("Received on_confirm payload:", json.dumps(data, indent=2))

            context = data.get("context", {})
            message_id = context.get("message_id")
            transaction_id = context.get("transaction_id")
            timestamp_str = context.get("timestamp")
            action = context.get("action")

            # Validate context fields
            if not all([message_id, transaction_id, timestamp_str, action]):
                return Response(
                    {
                        "message": {"ack": {"status": "NACK"}},
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            if action != "on_confirm":
                return Response(
                    {
                        "message": {"ack": {"status": "NACK"}},
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            # Validate timestamp
            timestamp = parse_datetime(timestamp_str)
            if not timestamp:
                return Response(
                    {
                        "context": context,
                        "message": {"ack": {"status": "NACK"}},
                        "error": {
                            "type": "TIMESTAMP-ERROR",
                            "message": "Invalid timestamp format",
                        },
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            # Validate transaction
            try:
                transaction = Transaction.objects.get(transaction_id=transaction_id)
            except Transaction.DoesNotExist:
                logger.warning("Transaction not found: %s", transaction_id)
                return Response(
                    {
                        "message": {"ack": {"status": "NACK"}},
                    },
                    status=status.HTTP_404_NOT_FOUND,
                )

            # Save to database
            OnConfirm.objects.create(
                transaction=transaction,
                message_id=message_id,
                payload=data,
                timestamp=timestamp,
            )

            try:
                send_to_analytics(schema_type="on_confirm", req_body=data)
            except Exception as e:
                logger.error(f"Observability logging failed: {str(e)}", exc_info=True)

        except Exception as e:
            logger.error("Failed to process on_confirm data: %s", str(e))
            return Response(
                {
                    "message": {"ack": {"status": "NACK"}},
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        return Response(
            {"message": {"ack": {"status": "ACK"}}}, status=status.HTTP_200_OK
        )


class OnStatusView(APIView):
    def post(self, request, *args, **kwargs):
        try:
            data = request.data
            logger.info("Received on_status payload: %s", data)
            print("Received on_status payload:", json.dumps(data, indent=2))

            context = data.get("context", {})
            message_id = context.get("message_id")
            transaction_id = context.get("transaction_id")
            timestamp_str = context.get("timestamp")
            action = context.get("action")

            # Validate context fields
            if not all([message_id, transaction_id, timestamp_str, action]):
                return Response(
                    {
                        "message": {"ack": {"status": "NACK"}},
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            if action != "on_status":
                return Response(
                    {
                        "message": {"ack": {"status": "NACK"}},
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            # Validate timestamp
            timestamp = parse_datetime(timestamp_str)
            if not timestamp:
                return Response(
                    {
                        "message": {"ack": {"status": "NACK"}},
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            # Validate transaction
            try:
                transaction = Transaction.objects.get(transaction_id=transaction_id)
            except Transaction.DoesNotExist:
                logger.warning("Transaction not found: %s", transaction_id)
                return Response(
                    {
                        "message": {"ack": {"status": "NACK"}},
                    },
                    status=status.HTTP_404_NOT_FOUND,
                )

            customer_pan = (
                data.get("message", {})
                .get("order", {})
                .get("fulfillments", [{}])[0]
                .get("customer", {})
                .get("person", {})
                .get("id", "")
            )
            if customer_pan.startswith("pan:"):
                customer_pan = customer_pan.split("pan:")[1]
            else:
                customer_pan = None

            # Save to database
            OnStatus.objects.create(
                transaction=transaction,
                message_id=message_id,
                payload=data,
                pan=customer_pan,
                timestamp=timestamp,
            )
            try:
                send_to_analytics(schema_type="on_status", req_body=data)
            except Exception as e:
                logger.error(f"Observability logging failed: {str(e)}", exc_info=True)

        except Exception as e:
            logger.error("Failed to process on_status data: %s", str(e))
            return Response(
                {
                    "message": {"ack": {"status": "NACK"}},
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        # Return success ACK
        return Response(
            {"message": {"ack": {"status": "ACK"}}}, status=status.HTTP_200_OK
        )


class OnUpdateView(APIView):

    def post(self, request, *args, **kwargs):
        try:
            data = request.data
            logger.info("Received on_update payload: %s", data)
            print("Received on_update payload:", json.dumps(data, indent=2))

            context = data.get("context", {})
            message_id = context.get("message_id")
            transaction_id = context.get("transaction_id")
            timestamp_str = context.get("timestamp")
            action = context.get("action")

            # Validate context fields
            if not all([message_id, transaction_id, timestamp_str, action]):
                return Response(
                    {
                        "message": {"ack": {"status": "NACK"}},
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            if action != "on_update":
                return Response(
                    {
                        "message": {"ack": {"status": "NACK"}},
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            # Validate timestamp
            timestamp = parse_datetime(timestamp_str)
            if not timestamp:
                return Response(
                    {
                        "message": {"ack": {"status": "NACK"}},
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            # Validate transaction
            try:
                transaction = Transaction.objects.get(transaction_id=transaction_id)
            except Transaction.DoesNotExist:
                logger.warning("Transaction not found: %s", transaction_id)
                return Response(
                    {
                        "message": {"ack": {"status": "NACK"}},
                    },
                    status=status.HTTP_404_NOT_FOUND,
                )

            # Save to database
            OnUpdate.objects.create(
                transaction=transaction,
                message_id=message_id,
                payload=data,
                timestamp=timestamp,
            )

            try:
                send_to_analytics(schema_type="on_update", req_body=data)
            except Exception as e:
                logger.error(f"Observability logging failed: {str(e)}", exc_info=True)

        except Exception as e:
            logger.error("Failed to process on_update data: %s", str(e))
            return Response(
                {
                    "message": {"ack": {"status": "NACK"}},
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        # Success response
        return Response(
            {"message": {"ack": {"status": "ACK"}}}, status=status.HTTP_200_OK
        )


# SIP Creation with Kyc


class DigiLockerFormSubmission(APIView):

    def post(self, request, *args, **kwargs):
        transaction_id = request.data.get("transaction_id")
        bpp_id = request.data.get("bpp_id")
        bpp_uri = request.data.get("bpp_uri")
        message_id_select = request.data.get("message_id_1")
        message_id = request.data.get("message_id")

        if not all([transaction_id, bpp_id, bpp_uri, message_id_select]):
            return Response(
                {"error": "Required all Fields"}, status=status.HTTP_400_BAD_REQUEST
            )

        obj = (
            OnStatus.objects.filter(transaction__transaction_id=transaction_id)
            .order_by("-timestamp")
            .first()
        )
        if not message_id:
            message_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat(sep="T", timespec="milliseconds") + "Z"

        try:
            provider = obj.payload["message"]["order"]["provider"]
            item = obj.payload["message"]["order"]["items"]
            xinput = obj.payload["message"]["order"]["xinput"]
            fulfillments = obj.payload["message"]["order"]["fulfillments"]
        except (KeyError, TypeError) as e:
            return Response(
                {"error": f"Missing key in payload: {e}"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        payload = {
            "context": {
                "location": {"country": {"code": "IND"}, "city": {"code": "*"}},
                "domain": "ONDC:FIS14",
                "timestamp": timestamp,
                "bap_id": BAP_ID,
                "bap_uri": BAP_URI,
                "transaction_id": transaction_id,
                "message_id": message_id,
                "version": "2.0.0",
                "ttl": "PT10M",
                "bpp_id": bpp_id,
                "bpp_uri": bpp_uri,
                "action": "select",
            },
            "message": {
                "order": {
                    "provider": {"id": provider["id"]},
                    "items": [
                        {
                            "id": item[0]["id"],
                            "quantity": {
                                "selected": {
                                    "measure": {
                                        "value": item[0]["quantity"]["selected"][
                                            "measure"
                                        ]["value"],
                                        "unit": item[0]["quantity"]["selected"][
                                            "measure"
                                        ]["unit"],
                                    }
                                }
                            },
                            "fulfillment_ids": [item[0]["fulfillment_ids"][0]],
                        }
                    ],
                    "fulfillments": [
                        {
                            "id": fulfillments[0]["id"],
                            "type": fulfillments[0]["type"],
                            "customer": {
                                "person": {
                                    "id": fulfillments[0]["customer"]["person"]["id"]
                                }
                            },
                            "agent": {
                                "person": {
                                    "id": fulfillments[0]["agent"]["person"]["id"]
                                },
                                "organization": {
                                    "creds": [
                                        {
                                            "id": fulfillments[0]["agent"][
                                                "organization"
                                            ]["creds"][0]["id"],
                                            "type": "ARN",
                                        },
                                    ]
                                },
                            },
                            "stops": [
                                {
                                    "time": {
                                        "schedule": {
                                            "frequency": fulfillments[0]["stops"][0][
                                                "time"
                                            ]["schedule"]["frequency"]
                                        }
                                    }
                                }
                            ],
                        }
                    ],
                    "xinput": {
                        "form": {"id": xinput["form"]["id"]},
                        "form_response": {
                            "submission_id": xinput["form_response"]["submission_id"]
                        },
                    },
                    "tags": [
                        {
                            "display": False,
                            "descriptor": {
                                "name": "BAP Terms of Engagement",
                                "code": "BAP_TERMS",
                            },
                            "list": [
                                {
                                    "descriptor": {
                                        "name": "Static Terms (Transaction Level)",
                                        "code": "STATIC_TERMS",
                                    },
                                    "value": obj.payload["message"]["order"]["tags"][0][
                                        "list"
                                    ][0]["value"],
                                },
                                {
                                    "descriptor": {
                                        "name": "Offline Contract",
                                        "code": "OFFLINE_CONTRACT",
                                    },
                                    "value": "true",
                                },
                            ],
                        }
                    ],
                }
            },
        }

        # Send to gateway

        request_body_str = json.dumps(payload, separators=(",", ":"))
        auth_header = create_authorisation_header(request_body=request_body_str)

        headers = {
            "Content-Type": "application/json",
            "Authorization": auth_header,
            "X-Gateway-Authorization": os.getenv("SIGNED_UNIQUE_REQ_ID", ""),
            "X-Gateway-Subscriber-Id": os.getenv("SUBSCRIBER_ID"),
        }

        response = requests.post(
            f"{bpp_uri}/select", data=request_body_str, headers=headers
        )
        try:
            send_to_analytics(schema_type="select", req_body=payload)
        except Exception as e:
            logger.error(f"Observability logging failed: {str(e)}", exc_info=True)
        return Response(
            {
                "status_code": response.status_code,
                "response": response.json() if response.content else {},
            },
            status=status.HTTP_200_OK,
        )


class EsignFormSubmission(APIView):
    def post(self, request, *args, **kwargs):
        transaction_id = request.data.get("transaction_id")
        bpp_id = request.data.get("bpp_id")
        bpp_uri = request.data.get("bpp_uri")
        message_id_select = request.data.get("message_id_select")
        message_id = request.data.get("message_id")

        if not all([transaction_id, bpp_id, bpp_uri]):
            return Response(
                {"error": "Required all Fields"}, status=status.HTTP_400_BAD_REQUEST
            )

        obj = (
            OnStatus.objects.filter(transaction__transaction_id=transaction_id)
            .order_by("-timestamp")
            .first()
        )
        if not message_id:
            message_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat(sep="T", timespec="milliseconds") + "Z"

        try:
            provider = obj.payload["message"]["order"]["provider"]
            item = obj.payload["message"]["order"]["items"]
            xinput = obj.payload["message"]["order"]["xinput"]
            fulfillments = obj.payload["message"]["order"]["fulfillments"]
        except (KeyError, TypeError) as e:
            return Response(
                {"error": f"Missing key in payload: {e}"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        payload = {
            "context": {
                "location": {"country": {"code": "IND"}, "city": {"code": "*"}},
                "domain": "ONDC:FIS14",
                "timestamp": timestamp,
                "bap_id": BAP_ID,
                "bap_uri": BAP_URI,
                "transaction_id": transaction_id,
                "message_id": message_id,
                "version": "2.0.0",
                "ttl": "PT10M",
                "bpp_id": bpp_id,
                "bpp_uri": bpp_uri,
                "action": "select",
            },
            "message": {
                "order": {
                    "provider": {"id": provider["id"]},
                    "items": [
                        {
                            "id": item[0]["id"],
                            "quantity": {
                                "selected": {
                                    "measure": {
                                        "value": item[0]["quantity"]["selected"][
                                            "measure"
                                        ]["value"],
                                        "unit": item[0]["quantity"]["selected"][
                                            "measure"
                                        ]["unit"],
                                    }
                                }
                            },
                            "fulfillment_ids": [item[0]["fulfillment_ids"][0]],
                        }
                    ],
                    "fulfillments": [
                        {
                            "id": fulfillments[0]["id"],
                            "type": fulfillments[0]["type"],
                            "customer": {
                                "person": {
                                    "id": fulfillments[0]["customer"]["person"]["id"]
                                }
                            },
                            "agent": {
                                "person": {
                                    "id": fulfillments[0]["agent"]["person"]["id"]
                                },
                                "organization": {
                                    "creds": [
                                        {
                                            "id": fulfillments[0]["agent"][
                                                "organization"
                                            ]["creds"][0]["id"],
                                            "type": "ARN",
                                        },
                                    ]
                                },
                            },
                            "stops": [
                                {
                                    "time": {
                                        "schedule": {
                                            "frequency": fulfillments[0]["stops"][0][
                                                "time"
                                            ]["schedule"]["frequency"]
                                        }
                                    }
                                }
                            ],
                        }
                    ],
                    "xinput": {
                        "form": {"id": xinput["form"]["id"]},
                        "form_response": {
                            "submission_id": xinput["form_response"]["submission_id"]
                        },
                    },
                    "tags": [
                        {
                            "display": False,
                            "descriptor": {
                                "name": "BAP Terms of Engagement",
                                "code": "BAP_TERMS",
                            },
                            "list": [
                                {
                                    "descriptor": {
                                        "name": "Static Terms (Transaction Level)",
                                        "code": "STATIC_TERMS",
                                    },
                                    "value": obj.payload["message"]["order"]["tags"][0][
                                        "list"
                                    ][0]["value"],
                                },
                                {
                                    "descriptor": {
                                        "name": "Offline Contract",
                                        "code": "OFFLINE_CONTRACT",
                                    },
                                    "value": "true",
                                },
                            ],
                        }
                    ],
                }
            },
        }
        # Send to gateway

        request_body_str = json.dumps(payload, separators=(",", ":"))
        auth_header = create_authorisation_header(request_body=request_body_str)

        headers = {
            "Content-Type": "application/json",
            "Authorization": auth_header,
            "X-Gateway-Authorization": os.getenv("SIGNED_UNIQUE_REQ_ID", ""),
            "X-Gateway-Subscriber-Id": os.getenv("SUBSCRIBER_ID"),
        }

        response = requests.post(
            f"{bpp_uri}/select", data=request_body_str, headers=headers
        )
        try:
            send_to_analytics(schema_type="select", req_body=payload)
        except Exception as e:
            logger.error(f"Observability logging failed: {str(e)}", exc_info=True)
        return Response(
            {
                "status_code": response.status_code,
                "response": response.json() if response.content else {},
            },
            status=status.HTTP_200_OK,
        )


# SIP Creation (Existing Folio - Investor selects/enters a folio)


class SIPExixstingInit(APIView):
    def post(self, request, *args, **kwargs):
        transaction_id = request.data.get("transaction_id")
        bpp_id = request.data.get("bpp_id")
        bpp_uri = request.data.get("bpp_uri")
        message_id = request.data.get("message_id")
        phone = str(request.data.get("phone"))
        ifsc = request.data.get("ifsc")
        payment_mode = request.data.get("payment_mode")
        account_number = request.data.get("account_number")

        if not all([transaction_id, bpp_id, bpp_uri, message_id, phone]):
            return Response(
                {
                    "error": "Missing transaction_id, bpp_id, or bpp_uri,message_id,phone"
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        obj = get_object_or_404(
            SelectSIP,
            payload__context__bpp_id=bpp_id,
            payload__context__bpp_uri=bpp_uri,
            transaction__transaction_id=transaction_id,
        )
        if not message_id:
            message_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat(sep="T", timespec="milliseconds") + "Z"

        try:
            provider = obj.payload["message"]["order"]["provider"]
            item = obj.payload["message"]["order"]["items"]
            fulfillments = obj.payload["message"]["order"]["fulfillments"]
            payments = obj.payload["message"]["order"]["payments"]
        except KeyError as e:
            return Response(
                {"error": f"Missing key in payload: {e}"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        except TypeError:
            return Response(
                {"error": "Invalid payload structure (possibly None or wrong type)"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        payload = {
            "context": {
                "location": {"country": {"code": "IND"}, "city": {"code": "*"}},
                "domain": "ONDC:FIS14",
                "timestamp": timestamp,
                "bap_id": BAP_ID,
                "bap_uri": BAP_URI,
                "transaction_id": transaction_id,
                "message_id": message_id,
                "version": "2.0.0",
                "ttl": "PT10M",
                "bpp_id": bpp_id,
                "bpp_uri": bpp_uri,
                "action": "init",
            },
            "message": {
                "order": {
                    "provider": {"id": provider["id"]},
                    "items": [
                        {
                            "id": item[0]["id"],
                            "quantity": {
                                "selected": {
                                    "measure": {
                                        "value": item[0]["quantity"]["selected"][
                                            "measure"
                                        ]["value"],
                                        "unit": item[0]["quantity"]["selected"][
                                            "measure"
                                        ]["unit"],
                                    }
                                }
                            },
                            "fulfillment_ids": [
                                obj.payload["message"]["order"]["quote"]["breakup"][0][
                                    "item"
                                ]["fulfillment_ids"][0]
                            ],
                        }
                    ],
                    "fulfillments": [
                        {
                            "id": fulfillments[0]["id"],
                            "type": fulfillments[0]["type"],
                            "customer": {
                                "person": {
                                    "id": fulfillments[0]["customer"]["person"]["id"],
                                    "creds": [
                                        {
                                            "id": fulfillments[0]["tags"][1]["list"][0][
                                                "value"
                                            ],
                                            "type": "FOLIO",
                                        },
                                        {
                                            "id": get_client_ip(request),
                                            "type": "IP_ADDRESS",
                                        },
                                    ],
                                },
                                "contact": {"phone": phone},
                            },
                            "agent": {
                                "person": {
                                    "id": fulfillments[0]["agent"]["person"]["id"]
                                },
                                "organization": {
                                    "creds": [
                                        {
                                            "id": fulfillments[0]["agent"][
                                                "organization"
                                            ]["creds"][0]["id"],
                                            "type": "ARN",
                                        },
                                    ]
                                },
                            },
                            "stops": [
                                {
                                    "time": {
                                        "schedule": {
                                            "frequency": fulfillments[0]["stops"][0][
                                                "time"
                                            ]["schedule"]["frequency"]
                                        }
                                    }
                                }
                            ],
                        }
                    ],
                    "payments": [
                        {
                            "collected_by": payments[0]["collected_by"],
                            "params": {
                                "amount": item[0]["quantity"]["selected"]["measure"][
                                    "value"
                                ],
                                "currency": item[0]["quantity"]["selected"]["measure"][
                                    "unit"
                                ],
                                "source_bank_code": str(ifsc),
                                "source_bank_account_number": str(account_number),
                                "source_bank_account_name": payments[1]["tags"][0][
                                    "list"
                                ][4]["value"],
                            },
                            "type": payments[0]["type"],
                            "tags": [
                                {
                                    "descriptor": {
                                        "name": "Payment Method",
                                        "code": "PAYMENT_METHOD",
                                    },
                                    "list": [
                                        {
                                            "descriptor": {"code": "MODE"},
                                            "value": payment_mode,
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                    "tags": [
                        {
                            "display": False,
                            "descriptor": {
                                "name": "BAP Terms of Engagement",
                                "code": "BAP_TERMS",
                            },
                            "list": [
                                {
                                    "descriptor": {
                                        "name": "Static Terms (Transaction Level)",
                                        "code": "STATIC_TERMS",
                                    },
                                    "value": "https://buyerapp.com/legal/ondc:fis14/static_terms?v=0.1",
                                },
                                {
                                    "descriptor": {
                                        "name": "Offline Contract",
                                        "code": "OFFLINE_CONTRACT",
                                    },
                                    "value": "true",
                                },
                            ],
                        }
                    ],
                }
            },
        }

        request_body_str = json.dumps(payload, separators=(",", ":"))
        auth_header = create_authorisation_header(request_body=request_body_str)

        headers = {
            "Content-Type": "application/json",
            "Authorization": auth_header,
            "X-Gateway-Authorization": os.getenv("SIGNED_UNIQUE_REQ_ID", ""),
            "X-Gateway-Subscriber-Id": os.getenv("SUBSCRIBER_ID"),
        }

        response = requests.post(
            f"{bpp_uri}/init", data=request_body_str, headers=headers
        )
        try:
            send_to_analytics(schema_type="init", req_body=payload)
        except Exception as e:
            logger.error(f"Observability logging failed: {str(e)}", exc_info=True)
        return Response(
            {
                "status_code": response.status_code,
                "response": response.json() if response.content else {},
            },
            status=status.HTTP_200_OK,
        )


class SIPExistingConfirm(APIView):
    def post(self, request, *args, **kwargs):
        transaction_id = request.data.get("transaction_id")
        bpp_id = request.data.get("bpp_id")
        bpp_uri = request.data.get("bpp_uri")
        message_id = request.data.get("message_id")

        if not all([transaction_id, bpp_id, bpp_uri, message_id]):
            return Response(
                {"error": "Missing transaction_id, bpp_id, or bpp_uri"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        obj = get_object_or_404(
            OnInitSIP,
            payload__context__bpp_id=bpp_id,
            payload__context__bpp_uri=bpp_uri,
            transaction__transaction_id=transaction_id,
        )

        if not message_id:
            message_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat(sep="T", timespec="milliseconds") + "Z"

        try:
            provider = obj.payload["message"]["order"]["provider"]
            item = obj.payload["message"]["order"]["items"]
            fulfillments = obj.payload["message"]["order"]["fulfillments"]
            payments = obj.payload["message"]["order"]["payments"]
        except KeyError as e:
            return Response(
                {"error": f"Missing key in payload: {e}"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        except TypeError:
            return Response(
                {"error": "Invalid payload structure (possibly None or wrong type)"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        payload = {
            "context": {
                "location": {"country": {"code": "IND"}, "city": {"code": "*"}},
                "domain": "ONDC:FIS14",
                "timestamp": timestamp,
                "bap_id": BAP_ID,
                "bap_uri": BAP_URI,
                "transaction_id": transaction_id,
                "message_id": message_id,
                "version": "2.0.0",
                "ttl": "PT10M",
                "bpp_id": bpp_id,
                "bpp_uri": bpp_uri,
                "action": "confirm",
            },
            "message": {
                "order": {
                    "id": obj.payload["message"]["order"]["id"],
                    "provider": {"id": provider["id"]},
                    "items": [
                        {
                            "id": item[0]["id"],
                            "quantity": {
                                "selected": {
                                    "measure": {
                                        "value": item[0]["quantity"]["selected"][
                                            "measure"
                                        ]["value"],
                                        "unit": item[0]["quantity"]["selected"][
                                            "measure"
                                        ]["unit"],
                                    }
                                }
                            },
                            "fulfillment_ids": [item[0]["fulfillment_ids"][0]],
                            "payment_ids": [item[0]["payment_ids"][0]],
                        }
                    ],
                    "fulfillments": [
                        {
                            "id": fulfillments[0]["id"],
                            "type": fulfillments[0]["type"],
                            "customer": {
                                "person": {
                                    "id": fulfillments[0]["customer"]["person"]["id"],
                                    "creds": [
                                        {
                                            "id": fulfillments[0]["customer"]["person"][
                                                "creds"
                                            ][0]["id"],
                                            "type": "FOLIO",
                                        },
                                        {
                                            "id": fulfillments[0]["customer"]["person"][
                                                "creds"
                                            ][1]["id"],
                                            "type": "IP_ADDRESS",
                                        },
                                    ],
                                },
                                "contact": {
                                    "phone": fulfillments[0]["customer"]["contact"][
                                        "phone"
                                    ]
                                },
                            },
                            "agent": {
                                "person": {
                                    "id": fulfillments[0]["agent"]["person"]["id"]
                                },
                                "organization": {
                                    "creds": [
                                        {
                                            "id": fulfillments[0]["agent"][
                                                "organization"
                                            ]["creds"][0]["id"],
                                            "type": "ARN",
                                        },
                                    ]
                                },
                            },
                            "stops": [
                                {
                                    "time": {
                                        "schedule": {
                                            "frequency": fulfillments[0]["stops"][0][
                                                "time"
                                            ]["schedule"]["frequency"]
                                        }
                                    }
                                }
                            ],
                        }
                    ],
                    "payments": [
                        {
                            "id": payments[0]["id"],
                            "collected_by": payments[0]["collected_by"],
                            "status": payments[0]["status"],
                            "params": {
                                "amount": payments[0]["params"]["amount"],
                                "currency": payments[0]["params"]["currency"],
                                "source_bank_code": payments[0]["params"][
                                    "source_bank_code"
                                ],
                                "source_bank_account_number": payments[0]["params"][
                                    "source_bank_account_number"
                                ],
                                "source_bank_account_name": payments[0]["params"][
                                    "source_bank_account_name"
                                ],
                                "transaction_id": payments[0]["id"],
                            },
                            "type": payments[0]["type"],
                            "tags": [
                                {
                                    "descriptor": {
                                        "name": "Payment Method",
                                        "code": "PAYMENT_METHOD",
                                    },
                                    "list": [
                                        {
                                            "descriptor": {"code": "MODE"},
                                            "value": payments[0]["tags"][0]["list"][0][
                                                "value"
                                            ],
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                    "tags": [
                        {
                            "display": False,
                            "descriptor": {
                                "name": "BAP Terms of Engagement",
                                "code": "BAP_TERMS",
                            },
                            "list": [
                                {
                                    "descriptor": {
                                        "name": "Static Terms (Transaction Level)",
                                        "code": "STATIC_TERMS",
                                    },
                                    "value": "https://buyerapp.com/legal/ondc:fis14/static_terms?v=0.1",
                                },
                                {
                                    "descriptor": {
                                        "name": "Offline Contract",
                                        "code": "OFFLINE_CONTRACT",
                                    },
                                    "value": "true",
                                },
                            ],
                        },
                        {
                            "display": False,
                            "descriptor": {
                                "name": "BPP Terms of Engagement",
                                "code": "BPP_TERMS",
                            },
                            "list": [
                                {
                                    "descriptor": {
                                        "name": "Static Terms (Transaction Level)",
                                        "code": "STATIC_TERMS",
                                    },
                                    "value": "https://sellerapp.com/legal/ondc:fis14/static_terms?v=0.1",
                                },
                                {
                                    "descriptor": {
                                        "name": "Offline Contract",
                                        "code": "OFFLINE_CONTRACT",
                                    },
                                    "value": "true",
                                },
                            ],
                        },
                    ],
                }
            },
        }
        request_body_str = json.dumps(payload, separators=(",", ":"))
        auth_header = create_authorisation_header(request_body=request_body_str)

        headers = {
            "Content-Type": "application/json",
            "Authorization": auth_header,
            "X-Gateway-Authorization": os.getenv("SIGNED_UNIQUE_REQ_ID", ""),
            "X-Gateway-Subscriber-Id": os.getenv("SUBSCRIBER_ID"),
        }

        response = requests.post(
            f"{bpp_uri}/confirm", data=request_body_str, headers=headers
        )
        try:
            send_to_analytics(schema_type="confirm", req_body=payload)
        except Exception as e:
            logger.error(f"Observability logging failed: {str(e)}", exc_info=True)
        return Response(
            {
                "status_code": response.status_code,
                "response": response.json() if response.content else {},
            },
            status=status.HTTP_200_OK,
        )


# Sip Cancel By tHe Investor
class SIPCancel(APIView):
    def post(self, request, *ags, **kwargs):
        transaction_id = request.data.get("transaction_id")
        bpp_id = request.data.get("bpp_id")
        bpp_uri = request.data.get("bpp_uri")
        message_id = request.data.get("message_id")
        order_id = request.data.get("order_id")
        # message_id=request.data.get('message_id')

        if not all([transaction_id, bpp_id, bpp_uri, order_id]):
            return Response(
                {"error": "Missing transaction_id, bpp_id, or bpp_uri"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        # obj=get_object_or_404(OnConfirm,payload__context__bpp_id=bpp_id,payload__context__bpp_uri=bpp_uri,transaction__transaction_id=transaction_id,payload__context__message_id=message_id)

        if not message_id:
            message_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat(sep="T", timespec="milliseconds") + "Z"

        # try:
        #     provider=obj.payload['message']['order']['provider']
        #     item=obj.payload['message']['order']['items']
        #     fulfillments=obj.payload['message']['order']['fulfillments']
        #     payments=obj.payload['message']['order']['payments']
        # except KeyError as e:
        #     return Response(
        #         {"error": f"Missing key in payload: {e}"},
        #         status=status.HTTP_400_BAD_REQUEST
        #     )
        # except TypeError:
        #     return Response(
        #         {"error": "Invalid payload structure (possibly None or wrong type)"},
        #         status=status.HTTP_400_BAD_REQUEST
        #     )

        payload = {
            "context": {
                "location": {"country": {"code": "IND"}, "city": {"code": "*"}},
                "domain": "ONDC:FIS14",
                "timestamp": timestamp,
                "bap_id": BAP_ID,
                "bap_uri": BAP_URI,
                "transaction_id": transaction_id,
                "message_id": message_id,
                "version": "2.0.0",
                "ttl": "PT10M",
                "bpp_id": bpp_id,
                "bpp_uri": bpp_uri,
                "action": "cancel",
            },
            "message": {
                "order_id": order_id,
                "cancellation_reason_id": "07",
                "tags": [
                    {
                        "display": True,
                        "descriptor": {
                            "name": "Consumer Info",
                            "code": "CONSUMER_INFO",
                        },
                        "list": [
                            {
                                "descriptor": {
                                    "name": "IP Address",
                                    "code": "IP_ADDRESS",
                                },
                                "value": get_client_ip(request),
                            }
                        ],
                    }
                ],
            },
        }
        request_body_str = json.dumps(payload, separators=(",", ":"))
        auth_header = create_authorisation_header(request_body=request_body_str)

        headers = {
            "Content-Type": "application/json",
            "Authorization": auth_header,
            "X-Gateway-Authorization": os.getenv("SIGNED_UNIQUE_REQ_ID", ""),
            "X-Gateway-Subscriber-Id": os.getenv("SUBSCRIBER_ID"),
        }

        response = requests.post(
            f"{bpp_uri}/cancel", data=request_body_str, headers=headers
        )
        try:
            send_to_analytics(schema_type="cancel", req_body=payload)
        except Exception as e:
            logger.error(f"Observability logging failed: {str(e)}", exc_info=True)
        return Response(
            {
                "status_code": response.status_code,
                "response": response.json() if response.content else {},
            },
            status=status.HTTP_200_OK,
        )


class StatusAPIView(APIView):

    def post(self, request, *args, **kwargs):

        transaction_id = request.data.get("transaction_id")
        message_id = request.data.get("message_id")
        bpp_id = request.data.get("bpp_id")
        bpp_uri = request.data.get("bpp_uri")
        order_id = request.data.get("order_id")

        if not all([transaction_id, bpp_id, bpp_uri]):
            return Response(
                {"error": "Missing transaction_id, bpp_id, or bpp_uri"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not message_id:
            message_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat(sep="T", timespec="milliseconds") + "Z"

        payload = {
            "context": {
                "location": {"country": {"code": "IND"}, "city": {"code": "*"}},
                "domain": "ONDC:FIS14",
                "timestamp": timestamp,
                "bap_id": BAP_ID,
                "bap_uri": BAP_URI,
                "transaction_id": transaction_id,
                "message_id": message_id,
                "version": "2.0.0",
                "ttl": "PT10M",
                "bpp_id": bpp_id,
                "bpp_uri": bpp_uri,
                "action": "status",
            },
            "message": {"order_id": order_id},
        }
        request_body_str = json.dumps(payload, separators=(",", ":"))
        auth_header = create_authorisation_header(request_body=request_body_str)

        headers = {
            "Content-Type": "application/json",
            "Authorization": auth_header,
            "X-Gateway-Authorization": os.getenv("SIGNED_UNIQUE_REQ_ID", ""),
            "X-Gateway-Subscriber-Id": os.getenv("SUBSCRIBER_ID"),
        }

        response = requests.post(
            f"{bpp_uri}/status", data=request_body_str, headers=headers
        )
        try:
            send_to_analytics(schema_type="status", req_body=payload)
        except Exception as e:
            logger.error(f"Observability logging failed: {str(e)}", exc_info=True)
        return Response(
            {
                "status_code": response.status_code,
                "response": response.json() if response.content else {},
            },
            status=status.HTTP_200_OK,
        )


class OnCancelView(APIView):
    def post(self, request, *args, **kwargs):
        try:
            data = request.data
            logger.info("Received on_confirm payload: %s", data)
            print("Received on_cancel payload:", json.dumps(data, indent=2))

            context = data.get("context", {})
            message_id = context.get("message_id")
            transaction_id = context.get("transaction_id")
            timestamp_str = context.get("timestamp")
            action = context.get("action")

            # Validate context fields
            if not all([message_id, transaction_id, timestamp_str, action]):
                return Response(
                    {
                        "message": {"ack": {"status": "NACK"}},
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            if action != "on_cancel":
                return Response(
                    {
                        "message": {"ack": {"status": "NACK"}},
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            # Validate timestamp
            timestamp = parse_datetime(timestamp_str)
            if not timestamp:
                return Response(
                    {
                        "message": {"ack": {"status": "NACK"}},
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            # Validate transaction
            try:
                transaction = Transaction.objects.get(transaction_id=transaction_id)
            except Transaction.DoesNotExist:
                logger.warning("Transaction not found: %s", transaction_id)
                return Response(
                    {
                        "message": {"ack": {"status": "NACK"}},
                    },
                    status=status.HTTP_404_NOT_FOUND,
                )

            # Save to database
            OnCancel.objects.create(
                transaction=transaction,
                message_id=message_id,
                payload=data,
                timestamp=timestamp,
            )
            try:
                send_to_analytics(schema_type="on_cancel", req_body=data)
            except Exception as e:
                logger.error(f"Observability logging failed: {str(e)}", exc_info=True)

        except Exception as e:
            logger.error("Failed to process on_cancel data: %s", str(e))
            return Response(
                {
                    "message": {"ack": {"status": "NACK"}},
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        # Success response
        return Response(
            {"message": {"ack": {"status": "ACK"}}}, status=status.HTTP_200_OK
        )


# Lumpsum - New Folio


# class Lumpsum(APIView):

#     def post(self, request, *args, **kwargs):
#         transaction_id = request.data.get("transaction_id")
#         bpp_id = request.data.get("bpp_id")
#         bpp_uri = request.data.get("bpp_uri")
#         isin=request.data.get("isin")
#         preferred_type = "LUMPSUM"
#         amount = request.data.get("amount", "3000")
#         pan = request.data.get("pan", "ABCDE1234F")
#         message_id = request.data.get("message_id")

#         if not all([ bpp_id, bpp_uri]):
#             return Response(
#                 {"error": "Required all Fields"}, status=status.HTTP_400_BAD_REQUEST
#             )

#         # obj = get_object_or_404(
#         #     FullOnSearch,
#         #     payload__context__bpp_id=bpp_id,
#         #     payload__context__bpp_uri=bpp_uri,
#         #     isin=isin
#         #     # transaction__transaction_id=transaction_id,
#         # )

#         obj= get_object_or_404(
#                 Scheme,
#                 isin=isin)
#         if not transaction_id:
#             transaction_id=str(uuid.uuid4())

#         if not message_id:
#             message_id = str(uuid.uuid4())
#         timestamp = datetime.utcnow().isoformat(sep="T", timespec="milliseconds") + "Z"
#         print(obj.payload)

#         # Get the first provider and item
#         provider = obj.payload["message"]["catalog"]["providers"]
#         matching_fulfillment = next(
#             (f for f in provider[0]["fulfillments"] if f.get("type") == preferred_type),
#             None,
#         )

#         payload = {
#             "context": {
#                 "location": {"country": {"code": "IND"}, "city": {"code": "*"}},
#                 "domain": "ONDC:FIS14",
#                 "timestamp": timestamp,
#                 "bap_id": BAP_ID,
#                 "bap_uri": BAP_URI,
#                 "transaction_id": transaction_id,
#                 "message_id": message_id,
#                 "version": "2.0.0",
#                 "ttl": "PT10M",
#                 "bpp_id": bpp_id,
#                 "bpp_uri": bpp_uri,
#                 "action": "select",
#             },
#             "message": {
#                 "order": {
#                     "provider": {"id": provider[0]["id"]},
#                     "items": [
#                         {
#                             "id": provider[0]["items"][0]["id"],
#                             "quantity": {
#                                 "selected": {
#                                     "measure": {"value": amount, "unit": "INR"}
#                                 }
#                             },
#                             "fulfillment_ids": [provider[0]["fulfillments"][0]["id"]],
#                         }
#                     ],
#                     "fulfillments": [
#                         {
#                             "id": matching_fulfillment["id"],
#                             "type": matching_fulfillment["type"],
#                             "customer": {
#                                 "person": {
#                                     "id": "pan:" + pan,
#                                 }
#                             },
#                             "agent": {
#                                 "person": {"id": os.getenv("EUIN")},
#                                 "organization": {
#                                     "creds": [
#                                         {"id": os.getenv("ARN"), "type": "ARN"},
#                                     ]
#                                 },
#                             },
#                         }
#                     ],
#                     "tags": [
#                         {
#                             "display": False,
#                             "descriptor": {
#                                 "name": "BAP Terms of Engagement",
#                                 "code": "BAP_TERMS",
#                             },
#                             "list": [
#                                 {
#                                     "descriptor": {
#                                         "name": "Static Terms (Transaction Level)",
#                                         "code": "STATIC_TERMS",
#                                     },
#                                     "value": "https://buyerapp.com/legal/ondc:fis14/static_terms?v=0.1",
#                                 },
#                                 {
#                                     "descriptor": {
#                                         "name": "Offline Contract",
#                                         "code": "OFFLINE_CONTRACT",
#                                     },
#                                     "value": "true",
#                                 },
#                             ],
#                         }
#                     ],
#                 }
#             },
#         }

#         transaction = Transaction.objects.get(transaction_id=transaction_id)
#         Message.objects.create(
#             transaction=transaction,
#             message_id=message_id,
#             action="select",
#             timestamp=parse_datetime(timestamp),
#             payload=payload,
#         )

#         # Send to gateway
#         request_body_str = json.dumps(payload, separators=(",", ":"))
#         auth_header = create_authorisation_header(request_body=request_body_str)

#         headers = {
#             "Content-Type": "application/json",
#             "Authorization": auth_header,
#             "X-Gateway-Authorization": os.getenv("SIGNED_UNIQUE_REQ_ID", ""),
#             "X-Gateway-Subscriber-Id": os.getenv("SUBSCRIBER_ID"),
#         }

#         response = requests.post(
#             f"{bpp_uri}/select", data=request_body_str, headers=headers
#         )
#         try:
#             send_to_analytics(schema_type="select", req_body=payload)
#         except Exception as e:
#             logger.error(f"Observability logging failed: {str(e)}", exc_info=True)

#         return Response(
#             {
#                 "status_code": response.status_code,
#                 "response": response.json() if response.content else {},
#             },
#             status=status.HTTP_200_OK,
#         )


class Lumpsum(APIView):
    
    def post(self, request, *args, **kwargs):
        # Extract request data
        transaction_id = request.data.get("transaction_id")
        bpp_id = request.data.get("bpp_id")
        bpp_uri = request.data.get("bpp_uri")
        isin = request.data.get("isin")
        preferred_type = "LUMPSUM"
        amount = request.data.get("amount", "3000")
        pan = request.data.get("pan", "ABCDE1234F")
        message_id = request.data.get("message_id")

        # Validation
        if not all([bpp_id, bpp_uri, isin]):
            return Response(
                {"error": "Required fields: bpp_id, bpp_uri, isin"}, 
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            # Method 1: Use the new ONDC models (Recommended)
            scheme_plan = self.get_scheme_plan_from_ondc_models(isin, bpp_id, preferred_type, amount)
            
            if not scheme_plan:
                # Method 2: Fallback to existing Scheme model
                scheme_plan = self.get_scheme_plan_from_existing_model(isin, preferred_type)
            
            if not scheme_plan:
                return Response(
                    {"error": "Scheme not found or no matching fulfillment options"}, 
                    status=status.HTTP_404_NOT_FOUND
                )

        except Exception as e:
            logger.error(f"Error fetching scheme: {str(e)}", exc_info=True)
            return Response(
                {"error": "Internal server error"}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        # Generate IDs if not provided
        if not transaction_id:
            transaction_id = str(uuid.uuid4())
        if not message_id:
            message_id = str(uuid.uuid4())
            
        timestamp = datetime.utcnow().isoformat(sep="T", timespec="milliseconds") + "Z"

        # Create or get ONDC transaction
        ondc_transaction = self.create_or_get_ondc_transaction(
            transaction_id, message_id, bpp_id, bpp_uri, timestamp
        )

        # Build payload using scheme data
        payload = self.build_select_payload(
            scheme_plan, transaction_id, message_id, timestamp, 
            bpp_id, bpp_uri, amount, pan, preferred_type
        )

        # Create/Update your existing transaction and message records
        self.create_transaction_records(transaction_id, message_id, timestamp, payload)

        # Send to gateway
        response = self.send_to_gateway(bpp_uri, payload)
        
        # Analytics logging
        try:
            send_to_analytics(schema_type="select", req_body=payload)
        except Exception as e:
            logger.error(f"Observability logging failed: {str(e)}", exc_info=True)

        return Response(
            {
                "status_code": response.status_code,
                "response": response.json() if response.content else {},
                "ondc_transaction_id": ondc_transaction.id if ondc_transaction else None,
            },
            status=status.HTTP_200_OK,
        )

    def get_scheme_plan_from_ondc_models(self, isin, bpp_id, preferred_type, amount):
        """
        Get scheme plan using the new ONDC models (Recommended approach)
        """
        try:
            # Find the scheme plan by ISIN and provider
            scheme_plan = SchemePlan.objects.select_related(
                'scheme__provider'
            ).prefetch_related(
                'fulfillment_options',
                'scheme__categories'
            ).get(
                isin=isin,
                scheme__provider__bpp_id=bpp_id,
                scheme__status='active'
            )
            
            # Check if there's a matching fulfillment option
            fulfillment_option = scheme_plan.fulfillment_options.filter(
                fulfillment_type=preferred_type
            ).first()
            
            if not fulfillment_option:
                logger.warning(f"No {preferred_type} fulfillment option found for ISIN: {isin}")
                return None
            
            # Validate amount against thresholds
            amount_float = float(amount)
            if (fulfillment_option.amount_min and amount_float < fulfillment_option.amount_min) or \
               (fulfillment_option.amount_max and amount_float > fulfillment_option.amount_max):
                logger.warning(f"Amount {amount} is outside allowed range for ISIN: {isin}")
                return None
            
            return {
                'type': 'ondc_model',
                'scheme_plan': scheme_plan,
                'fulfillment_option': fulfillment_option,
                'provider_data': {
                    'id': scheme_plan.scheme.provider.provider_id,
                    'name': scheme_plan.scheme.provider.name
                },
                'scheme_data': {
                    'id': scheme_plan.scheme.scheme_id,
                    'name': scheme_plan.scheme.name
                },
                'plan_data': {
                    'id': scheme_plan.plan_id,
                    'name': scheme_plan.name
                }
            }
            
        except SchemePlan.DoesNotExist:
            logger.info(f"Scheme plan not found in ONDC models for ISIN: {isin}")
            return None
        except Exception as e:
            logger.error(f"Error in get_scheme_plan_from_ondc_models: {str(e)}")
            return None

    def get_scheme_plan_from_existing_model(self, isin, preferred_type):
        """
        Fallback method using existing Scheme model
        """
        try:
            obj = get_object_or_404(Scheme, isin=isin)
            
            # Extract provider and fulfillment data from payload
            provider = obj.payload["message"]["catalog"]["providers"][0]
            
            matching_fulfillment = next(
                (f for f in provider["fulfillments"] if f.get("type") == preferred_type),
                None,
            )
            
            if not matching_fulfillment:
                return None
            
            return {
                'type': 'existing_model',
                'scheme_obj': obj,
                'provider_data': provider,
                'fulfillment_data': matching_fulfillment
            }
            
        except Exception as e:
            logger.error(f"Error in get_scheme_plan_from_existing_model: {str(e)}")
            return None

    def create_or_get_ondc_transaction(self, transaction_id, message_id, bpp_id, bpp_uri, timestamp):
        """
        Create or get ONDC transaction record
        """
        try:
            ondc_transaction, created = ONDCTransaction.objects.get_or_create(
                transaction_id=transaction_id,
                defaults={
                    'message_id': message_id,
                    'bap_id': os.getenv('BAP_ID', 'api.buyerapp.com'),
                    'bap_uri': os.getenv('BAP_URI', 'https://api.buyerapp.com/ondc'),
                    'bpp_id': bpp_id,
                    'bpp_uri': bpp_uri,
                    'domain': 'ONDC:FIS14',
                    'version': '2.0.0',
                    'action': 'select',
                    'country_code': 'IND',
                    'city_code': '*',
                    'timestamp': parse_datetime(timestamp),
                    'ttl': 'PT10M'
                }
            )
            
            if created:
                logger.info(f"Created new ONDC transaction: {transaction_id}")
            else:
                logger.info(f"Using existing ONDC transaction: {transaction_id}")
            
            return ondc_transaction
            
        except Exception as e:
            logger.error(f"Error creating ONDC transaction: {str(e)}")
            return None

    def build_select_payload(self, scheme_plan, transaction_id, message_id, timestamp, 
                           bpp_id, bpp_uri, amount, pan, preferred_type):
        """
        Build the select payload based on scheme plan data
        """
        if scheme_plan['type'] == 'ondc_model':
            # Use ONDC model data
            provider_id = scheme_plan['provider_data']['id']
            scheme_id = scheme_plan['scheme_data']['id']
            plan_id = scheme_plan['plan_data']['id']
            fulfillment_id = scheme_plan['fulfillment_option'].fulfillment_id
            
        else:
            # Use existing model data
            provider_data = scheme_plan['provider_data']
            provider_id = provider_data['id']
            scheme_id = provider_data['items'][0]['id']  # Assuming first item is scheme
            plan_id = provider_data['items'][1]['id'] if len(provider_data['items']) > 1 else scheme_id
            fulfillment_id = scheme_plan['fulfillment_data']['id']

        payload = {
            "context": {
                "location": {"country": {"code": "IND"}, "city": {"code": "*"}},
                "domain": "ONDC:FIS14",
                "timestamp": timestamp,
                "bap_id": os.getenv('BAP_ID', 'api.buyerapp.com'),
                "bap_uri": os.getenv('BAP_URI', 'https://api.buyerapp.com/ondc'),
                "transaction_id": transaction_id,
                "message_id": message_id,
                "version": "2.0.0",
                "ttl": "PT10M",
                "bpp_id": bpp_id,
                "bpp_uri": bpp_uri,
                "action": "select",
            },
            "message": {
                "order": {
                    "provider": {"id": provider_id},
                    "items": [
                        {
                            "id": plan_id,  # Use plan ID instead of scheme ID
                            "quantity": {
                                "selected": {
                                    "measure": {"value": amount, "unit": "INR"}
                                }
                            },
                            "fulfillment_ids": [fulfillment_id],
                        }
                    ],
                    "fulfillments": [
                        {
                            "id": fulfillment_id,
                            "type": preferred_type,
                            "customer": {
                                "person": {
                                    "id": "pan:" + pan,
                                }
                            },
                            "agent": {
                                "person": {"id": os.getenv("EUIN")},
                                "organization": {
                                    "creds": [
                                        {"id": os.getenv("ARN"), "type": "ARN"},
                                    ]
                                },
                            },
                        }
                    ],
                    "tags": [
                        {
                            "display": False,
                            "descriptor": {
                                "name": "BAP Terms of Engagement",
                                "code": "BAP_TERMS",
                            },
                            "list": [
                                {
                                    "descriptor": {
                                        "name": "Static Terms (Transaction Level)",
                                        "code": "STATIC_TERMS",
                                    },
                                    "value": "https://buyerapp.com/legal/ondc:fis14/static_terms?v=0.1",
                                },
                                {
                                    "descriptor": {
                                        "name": "Offline Contract",
                                        "code": "OFFLINE_CONTRACT",
                                    },
                                    "value": "true",
                                },
                            ],
                        }
                    ],
                }
            },
        }
        
        return payload

    def create_transaction_records(self, transaction_id, message_id, timestamp, payload):
        """
        Create or update your existing Transaction and Message records
        """
        try:
            # Create or get existing transaction
            transaction, created = Transaction.objects.get_or_create(
                transaction_id=transaction_id,
                defaults={
                    'created_at': parse_datetime(timestamp),
                    # Add other fields as needed
                }
            )
            
            # Create message record
            Message.objects.create(
                transaction=transaction,
                message_id=message_id,
                action="select",
                timestamp=parse_datetime(timestamp),
                payload=payload,
            )
            
            logger.info(f"Created transaction records for: {transaction_id}")
            
        except Exception as e:
            logger.error(f"Error creating transaction records: {str(e)}")

    def send_to_gateway(self, bpp_uri, payload):
        """
        Send request to gateway
        """
        request_body_str = json.dumps(payload, separators=(",", ":"))
        auth_header = create_authorisation_header(request_body=request_body_str)

        headers = {
            "Content-Type": "application/json",
            "Authorization": auth_header,
            "X-Gateway-Authorization": os.getenv("SIGNED_UNIQUE_REQ_ID", ""),
            "X-Gateway-Subscriber-Id": os.getenv("SUBSCRIBER_ID"),
        }

        response = requests.post(
            f"{bpp_uri}/select", data=request_body_str, headers=headers
        )
        
        return response


# Additional utility class for bulk operations
class ONDCTransactionManager:
    """
    Utility class for managing ONDC transactions and integrating with existing models
    """
    
    @classmethod
    def sync_existing_schemes_to_ondc(cls):
        """
        One-time migration script to sync existing Scheme data to ONDC models
        """
        existing_schemes = Scheme.objects.all()
        
        for scheme in existing_schemes:
            try:
                # Process the scheme payload and create ONDC models
                ONDCMutualFundService.process_ondc_response(scheme.payload)
                logger.info(f"Synced scheme: {scheme.isin}")
                
            except Exception as e:
                logger.error(f"Error syncing scheme {scheme.isin}: {str(e)}")
    
    @classmethod
    def get_scheme_by_isin(cls, isin, bpp_id=None):
        """
        Unified method to get scheme data from either ONDC models or existing models
        """
        # Try ONDC models first
        try:
            query = SchemePlan.objects.select_related('scheme__provider')
            if bpp_id:
                query = query.filter(scheme__provider__bpp_id=bpp_id)
            
            scheme_plan = query.get(isin=isin)
            return {
                'source': 'ondc',
                'scheme_plan': scheme_plan,
                'provider': scheme_plan.scheme.provider,
                'scheme': scheme_plan.scheme
            }
            
        except SchemePlan.DoesNotExist:
            pass
        
        # Fallback to existing models
        try:
            scheme = Scheme.objects.get(isin=isin)
            return {
                'source': 'existing',
                'scheme_obj': scheme
            }
            
        except Scheme.DoesNotExist:
            return None

    @classmethod
    def validate_investment_amount(cls, isin, amount, fulfillment_type, bpp_id=None):
        """
        Validate investment amount against scheme thresholds
        """
        scheme_data = cls.get_scheme_by_isin(isin, bpp_id)
        
        if not scheme_data:
            return False, "Scheme not found"
        
        amount_float = float(amount)
        
        if scheme_data['source'] == 'ondc':
            # Use ONDC model validation
            fulfillment = scheme_data['scheme_plan'].fulfillment_options.filter(
                fulfillment_type=fulfillment_type
            ).first()
            
            if not fulfillment:
                return False, f"No {fulfillment_type} option available"
            
            if fulfillment.amount_min and amount_float < fulfillment.amount_min:
                return False, f"Minimum amount is {fulfillment.amount_min}"
            
            if fulfillment.amount_max and amount_float > fulfillment.amount_max:
                return False, f"Maximum amount is {fulfillment.amount_max}"
            
            return True, "Valid"
        
        else:
            # Use existing model validation (extract from payload)
            # Implementation depends on your existing payload structure
            return True, "Valid"

class LumpFormSub(APIView):

    def post(self, request, *args, **kwargs):
        transaction_id = request.data.get("transaction_id")
        bpp_id = request.data.get("bpp_id")
        bpp_uri = request.data.get("bpp_uri")
        message_id = request.data.get("message_id")
        form_data = request.data.get("form_data")

        if not all([transaction_id, bpp_id, bpp_uri]):
            return Response(
                {"error": "Missing transaction_id, bpp_id, or bpp_uri"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        obj = get_object_or_404(
            SelectSIP,
            payload__context__bpp_id=bpp_id,
            payload__context__bpp_uri=bpp_uri,
            transaction__transaction_id=transaction_id,
        )

        if not message_id:
            message_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat(sep="T", timespec="milliseconds") + "Z"

        try:
            provider = obj.payload["message"]["order"]["provider"]
            item = obj.payload["message"]["order"]["items"]
            fulfillments = obj.payload["message"]["order"]["fulfillments"]
            xinput = obj.payload["message"]["order"]["xinput"]
            url = obj.payload["message"]["order"]["xinput"]["form"]["url"]

        except (KeyError, TypeError):
            return Response(
                {"error": "Form URL not found in payload"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        # user_kyc_data = {
        #     "pan": "ABCDE1234F",
        #     "dob": "1990-01-01",
        #     "email": "user@example.com",
        #     "name": "Ravi Kumar",
        #     "gender":"Male",
        #     "marital_status":"Married",
        #     "occupation":"Salaried",
        #     "source_of_wealth":"Business",
        #     "income_range":"1L to 5L",
        #     "cob":"India",
        #     "pob":"Kochi",
        #     "political_exposure":"no_exposure",
        #     "india_tax_residency_status":"resident",
        #     "mode_of_holding":"single",
        #     "ca_line":"hfjfk jifl jffj",

        # }
        try:
            res = requests.post(url, json=form_data)
            if res.status_code == 200:
                resp_json = res.json()
                submission_id = resp_json["submission_id"]
                if not submission_id:
                    return Response(
                        {"error": "submission id missing"},
                        status=status.HTTP_400_BAD_REQUEST,
                    )

                SubmissionID.objects.create(
                    transaction=obj.transaction,
                    submission_id=submission_id,
                    message_id=message_id,
                    timestamp=timestamp,
                )

                payload = {
                    "context": {
                        "location": {"country": {"code": "IND"}, "city": {"code": "*"}},
                        "domain": "ONDC:FIS14",
                        "timestamp": timestamp,
                        "bap_id": BAP_ID,
                        "bap_uri": BAP_URI,
                        "transaction_id": transaction_id,
                        "message_id": message_id,
                        "version": "2.0.0",
                        "ttl": "PT10M",
                        "bpp_id": bpp_id,
                        "bpp_uri": bpp_uri,
                        "action": "select",
                    },
                    "message": {
                        "order": {
                            "provider": {"id": provider["id"]},
                            "items": [
                                {
                                    "id": item[0]["id"],
                                    "quantity": {
                                        "selected": {
                                            "measure": {
                                                "value": item[0]["quantity"][
                                                    "selected"
                                                ]["measure"]["value"],
                                                "unit": item[0]["quantity"]["selected"][
                                                    "measure"
                                                ]["unit"],
                                            }
                                        }
                                    },
                                    "fulfillment_ids": [item[0]["fulfillment_ids"][0]],
                                }
                            ],
                            "fulfillments": [
                                {
                                    "id": fulfillments[0]["id"],
                                    "type": fulfillments[0]["type"],
                                    "customer": {
                                        "person": {
                                            "id": fulfillments[0]["customer"]["person"][
                                                "id"
                                            ]
                                        }
                                    },
                                    "agent": {
                                        "person": {
                                            "id": fulfillments[0]["agent"]["person"][
                                                "id"
                                            ]
                                        },
                                        "organization": {
                                            "creds": [
                                                {
                                                    "id": fulfillments[0]["agent"][
                                                        "organization"
                                                    ]["creds"][0]["id"],
                                                    "type": "ARN",
                                                },
                                            ]
                                        },
                                    },
                                }
                            ],
                            "xinput": {
                                "form": {"id": xinput["form"]["id"]},
                                "form_response": {"submission_id": submission_id},
                            },
                            "tags": [
                                {
                                    "display": False,
                                    "descriptor": {
                                        "name": "BAP Terms of Engagement",
                                        "code": "BAP_TERMS",
                                    },
                                    "list": [
                                        {
                                            "descriptor": {
                                                "name": "Static Terms (Transaction Level)",
                                                "code": "STATIC_TERMS",
                                            },
                                            "value": "https://buyerapp.com/legal/ondc:fis14/static_terms?v=0.1",
                                        },
                                        {
                                            "descriptor": {
                                                "name": "Offline Contract",
                                                "code": "OFFLINE_CONTRACT",
                                            },
                                            "value": "true",
                                        },
                                    ],
                                }
                            ],
                        }
                    },
                }

                # Send to gateway
                request_body_str = json.dumps(payload, separators=(",", ":"))
                auth_header = create_authorisation_header(request_body=request_body_str)

                headers = {
                    "Content-Type": "application/json",
                    "Authorization": auth_header,
                    "X-Gateway-Authorization": os.getenv("SIGNED_UNIQUE_REQ_ID", ""),
                    "X-Gateway-Subscriber-Id": os.getenv("SUBSCRIBER_ID"),
                }

                response = requests.post(
                    f"{bpp_uri}/select", data=request_body_str, headers=headers
                )
                try:
                    send_to_analytics(schema_type="select", req_body=payload)
                except Exception as e:
                    logger.error(
                        f"Observability logging failed: {str(e)}", exc_info=True
                    )
                return Response(
                    {
                        "status_code": response.status_code,
                        "response": response.json() if response.content else {},
                    },
                    status=status.HTTP_200_OK,
                )
            else:
                return Response(
                    {"error": f"Form upload failed with status {res.status_code}"},
                    status=status.HTTP_400_BAD_REQUEST,
                )
        except requests.exceptions.RequestException as e:
            return Response(
                {"error": f"Form upload failed: {str(e)}"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        except Exception as e:
            return Response(
                {"error": f"Unexpected error: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


class LumpINIT(APIView):

    def post(self, request, *args, **kwargs):
        transaction_id = request.data.get("transaction_id")
        bpp_id = request.data.get("bpp_id")
        bpp_uri = request.data.get("bpp_uri")
        message_id_select = request.data.get("message_id_select")
        name = request.data.get("name", "Ravi Kumar")
        phone = request.data.get("phone", "123456789")
        ifsc = request.data.get("ifsc", "HDFC0000089")
        account_number = request.data.get("account_number", "004701563111")
        message_id = request.data.get("message_id")
        payment_mode = request.data.get("payment_mode")

        if not all([transaction_id, bpp_id, bpp_uri, message_id_select]):
            return Response(
                {"error": "Required all Fields"}, status=status.HTTP_400_BAD_REQUEST
            )

        obj = get_object_or_404(
            SelectSIP,
            payload__context__bpp_id=bpp_id,
            payload__context__bpp_uri=bpp_uri,
            transaction__transaction_id=transaction_id,
            payload__context__message_id=message_id_select,
        )
        if not message_id:
            message_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat(sep="T", timespec="milliseconds") + "Z"

        try:
            provider = obj.payload["message"]["order"]["provider"]
            item = obj.payload["message"]["order"]["items"]
            fulfillments = obj.payload["message"]["order"]["fulfillments"]
            payments = obj.payload["message"]["order"]["payments"]
        except (KeyError, TypeError):
            return Response(
                {"error": "Form URL not found in payload"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        payload = {
            "context": {
                "location": {"country": {"code": "IND"}, "city": {"code": "*"}},
                "domain": "ONDC:FIS14",
                "timestamp": timestamp,
                "bap_id": BAP_ID,
                "bap_uri": BAP_URI,
                "transaction_id": transaction_id,
                "message_id": message_id,
                "version": "2.0.0",
                "ttl": "PT10M",
                "bpp_id": bpp_id,
                "bpp_uri": bpp_uri,
                "action": "init",
            },
            "message": {
                "order": {
                    "provider": {"id": provider["id"]},
                    "items": [
                        {
                            "id": item[0]["id"],
                            "quantity": {
                                "selected": {
                                    "measure": {
                                        "value": item[0]["quantity"]["selected"][
                                            "measure"
                                        ]["value"],
                                        "unit": item[0]["quantity"]["selected"][
                                            "measure"
                                        ]["unit"],
                                    }
                                }
                            },
                            "fulfillment_ids": [item[0]["fulfillment_ids"][0]],
                        }
                    ],
                    "fulfillments": [
                        {
                            "id": fulfillments[0]["id"],
                            "type": fulfillments[0]["type"],
                            "customer": {
                                "person": {
                                    "id": fulfillments[0]["customer"]["person"]["id"],
                                    "creds": [
                                        {
                                            "id": get_client_ip(request),
                                            "type": "IP_ADDRESS",
                                        }
                                    ],
                                },
                                "contact": {"phone": phone},
                            },
                            "agent": {
                                "person": {
                                    "id": fulfillments[0]["agent"]["person"]["id"],
                                },
                                "organization": {
                                    "creds": [
                                        {
                                            "id": fulfillments[0]["agent"][
                                                "organization"
                                            ]["creds"][0]["id"],
                                            "type": "ARN",
                                        },
                                    ]
                                },
                            },
                        }
                    ],
                    "payments": [
                        {
                            "collected_by": payments[0]["collected_by"],
                            "params": {
                                "amount": item[0]["quantity"]["selected"]["measure"][
                                    "value"
                                ],
                                "currency": "INR",
                                "source_bank_code": ifsc,
                                "source_bank_account_number": account_number,
                                "source_bank_account_name": name,
                            },
                            "type": payments[0]["type"],
                            "tags": [
                                {
                                    "descriptor": {
                                        "name": "Payment Method",
                                        "code": "PAYMENT_METHOD",
                                    },
                                    "list": [
                                        {
                                            "descriptor": {"code": "MODE"},
                                            "value": payment_mode,
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                    "tags": [
                        {
                            "display": False,
                            "descriptor": {
                                "name": "BAP Terms of Engagement",
                                "code": "BAP_TERMS",
                            },
                            "list": [
                                {
                                    "descriptor": {
                                        "name": "Static Terms (Transaction Level)",
                                        "code": "STATIC_TERMS",
                                    },
                                    "value": "https://buyerapp.com/legal/ondc:fis14/static_terms?v=0.1",
                                },
                                {
                                    "descriptor": {
                                        "name": "Offline Contract",
                                        "code": "OFFLINE_CONTRACT",
                                    },
                                    "value": "true",
                                },
                            ],
                        }
                    ],
                }
            },
        }

        request_body_str = json.dumps(payload, separators=(",", ":"))
        auth_header = create_authorisation_header(request_body=request_body_str)

        headers = {
            "Content-Type": "application/json",
            "Authorization": auth_header,
            "X-Gateway-Authorization": os.getenv("SIGNED_UNIQUE_REQ_ID", ""),
            "X-Gateway-Subscriber-Id": os.getenv("SUBSCRIBER_ID"),
        }

        response = requests.post(
            f"{bpp_uri}/init", data=request_body_str, headers=headers
        )
        try:
            send_to_analytics(schema_type="init", req_body=payload)
        except Exception as e:
            logger.error(f"Observability logging failed: {str(e)}", exc_info=True)
        return Response(
            {
                "status_code": response.status_code,
                "response": response.json() if response.content else {},
            },
            status=status.HTTP_200_OK,
        )


class ConfirmLump(APIView):
    def post(self, request, *args, **kwargs):
        transaction_id = request.data.get("transaction_id")
        bpp_id = request.data.get("bpp_id")
        bpp_uri = request.data.get("bpp_uri")
        message_id_init = request.data.get("message_id_init")
        message_id = request.data.get("message_id")

        if not all([transaction_id, bpp_id, bpp_uri, message_id_init]):
            return Response(
                {"error": "Required all Fields"}, status=status.HTTP_400_BAD_REQUEST
            )

        obj = get_object_or_404(
            OnInitSIP,
            payload__context__bpp_id=bpp_id,
            payload__context__bpp_uri=bpp_uri,
            transaction__transaction_id=transaction_id,
            payload__context__message_id=message_id_init,
        )

        if not message_id:
            message_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat(sep="T", timespec="milliseconds") + "Z"

        try:
            id = obj.payload["message"]["order"]["id"]
            provider = obj.payload["message"]["order"]["provider"]
            item = obj.payload["message"]["order"]["items"]
            fulfillments = obj.payload["message"]["order"]["fulfillments"]
            payments = obj.payload["message"]["order"]["payments"]
            url = payments[0]["url"]
        except (KeyError, TypeError) as e:
            return Response(
                {"error": f"Missing key in payload: {e}"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        payload = {
            "context": {
                "location": {"country": {"code": "IND"}, "city": {"code": "*"}},
                "domain": "ONDC:FIS14",
                "timestamp": timestamp,
                "bap_id": BAP_ID,
                "bap_uri": BAP_URI,
                "transaction_id": transaction_id,
                "message_id": message_id,
                "version": "2.0.0",
                "ttl": "PT10M",
                "bpp_id": bpp_id,
                "bpp_uri": bpp_uri,
                "action": "confirm",
            },
            "message": {
                "order": {
                    "id": id,
                    "provider": {"id": provider["id"]},
                    "items": [
                        {
                            "id": item[0]["id"],
                            "quantity": {
                                "selected": {
                                    "measure": {
                                        "value": item[0]["quantity"]["selected"][
                                            "measure"
                                        ]["value"],
                                        "unit": item[0]["quantity"]["selected"][
                                            "measure"
                                        ]["unit"],
                                    }
                                }
                            },
                            "fulfillment_ids": [item[0]["fulfillment_ids"][0]],
                            "payment_ids": [item[0]["payment_ids"][0]],
                        }
                    ],
                    "fulfillments": [
                        {
                            "id": fulfillments[0]["id"],
                            "type": fulfillments[0]["type"],
                            "customer": {
                                "person": {
                                    "id": fulfillments[0]["customer"]["person"]["id"],
                                    "creds": [
                                        {
                                            "id": fulfillments[0]["customer"]["person"][
                                                "creds"
                                            ][0]["id"],
                                            "type": "IP_ADDRESS",
                                        }
                                    ],
                                },
                                "contact": {
                                    "phone": fulfillments[0]["customer"]["contact"][
                                        "phone"
                                    ]
                                },
                            },
                            "agent": {
                                "person": {
                                    "id": fulfillments[0]["agent"]["person"]["id"]
                                },
                                "organization": {
                                    "creds": [
                                        {
                                            "id": fulfillments[0]["agent"][
                                                "organization"
                                            ]["creds"][0]["id"],
                                            "type": "ARN",
                                        },
                                    ]
                                },
                            },
                        }
                    ],
                    "payments": [
                        {
                            "id": payments[0]["id"],
                            "collected_by": payments[0]["collected_by"],
                            "status": payments[0]["status"],
                            "params": {
                                "amount": payments[0]["params"]["amount"],
                                "currency": payments[0]["params"]["currency"],
                                "source_bank_code": payments[0]["params"][
                                    "source_bank_code"
                                ],
                                "source_bank_account_number": payments[0]["params"][
                                    "source_bank_account_number"
                                ],
                                "source_bank_account_name": payments[0]["params"][
                                    "source_bank_account_name"
                                ],
                            },
                            "type": "PRE_FULFILLMENT",
                            "tags": [
                                {
                                    "descriptor": {
                                        "name": "Payment Method",
                                        "code": "PAYMENT_METHOD",
                                    },
                                    "list": [
                                        {
                                            "descriptor": {"code": "MODE"},
                                            "value": payments[0]["tags"][0]["list"][0][
                                                "value"
                                            ],
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                    "tags": [
                        {
                            "display": False,
                            "descriptor": {
                                "name": "BAP Terms of Engagement",
                                "code": "BAP_TERMS",
                            },
                            "list": [
                                {
                                    "descriptor": {
                                        "name": "Static Terms (Transaction Level)",
                                        "code": "STATIC_TERMS",
                                    },
                                    "value": "https://buyerapp.com/legal/ondc:fis14/static_terms?v=0.1",
                                },
                                {
                                    "descriptor": {
                                        "name": "Offline Contract",
                                        "code": "OFFLINE_CONTRACT",
                                    },
                                    "value": "true",
                                },
                            ],
                        },
                        {
                            "display": False,
                            "descriptor": {
                                "name": "BPP Terms of Engagement",
                                "code": "BPP_TERMS",
                            },
                            "list": [
                                {
                                    "descriptor": {
                                        "name": "Static Terms (Transaction Level)",
                                        "code": "STATIC_TERMS",
                                    },
                                    "value": "https://sellerapp.com/legal/ondc:fis14/static_terms?v=0.1",
                                },
                                {
                                    "descriptor": {
                                        "name": "Offline Contract",
                                        "code": "OFFLINE_CONTRACT",
                                    },
                                    "value": "true",
                                },
                            ],
                        },
                    ],
                }
            },
        }
        request_body_str = json.dumps(payload, separators=(",", ":"))
        auth_header = create_authorisation_header(request_body=request_body_str)

        headers = {
            "Content-Type": "application/json",
            "Authorization": auth_header,
            "X-Gateway-Authorization": os.getenv("SIGNED_UNIQUE_REQ_ID", ""),
            "X-Gateway-Subscriber-Id": os.getenv("SUBSCRIBER_ID"),
        }

        response = requests.post(
            f"{bpp_uri}/confirm", data=request_body_str, headers=headers
        )
        try:
            send_to_analytics(schema_type="confirm", req_body=payload)
        except Exception as e:
            logger.error(f"Observability logging failed: {str(e)}", exc_info=True)
        return Response(
            {
                "status_code": response.status_code,
                "response": response.json() if response.content else {},
            },
            status=status.HTTP_200_OK,
        )


# Lumpsum With KYC New Folio


class LumpsumDigiLockerSubmission(APIView):

    def post(self, request, *args, **kwargs):
        transaction_id = request.data.get("transaction_id")
        bpp_id = request.data.get("bpp_id")
        bpp_uri = request.data.get("bpp_uri")
        message_id_select = request.data.get("message_id_select")
        message_id = request.data.get("message_id")

        if not all([transaction_id, bpp_id, bpp_uri, message_id_select]):
            return Response(
                {"error": "Required all Fields"}, status=status.HTTP_400_BAD_REQUEST
            )

        obj = (
            OnStatus.objects.filter(transaction__transaction_id=transaction_id)
            .order_by("-timestamp")
            .first()
        )
        if not message_id:
            message_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat(sep="T", timespec="milliseconds") + "Z"

        try:
            provider = obj.payload["message"]["order"]["provider"]
            item = obj.payload["message"]["order"]["items"]
            xinput = obj.payload["message"]["order"]["xinput"]
            fulfillments = obj.payload["message"]["order"]["fulfillments"]
        except (KeyError, TypeError) as e:
            return Response(
                {"error": f"Missing key in payload: {e}"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        payload = {
            "context": {
                "location": {"country": {"code": "IND"}, "city": {"code": "*"}},
                "domain": "ONDC:FIS14",
                "timestamp": timestamp,
                "bap_id": BAP_ID,
                "bap_uri": BAP_URI,
                "transaction_id": transaction_id,
                "message_id": message_id,
                "version": "2.0.0",
                "ttl": "PT10M",
                "bpp_id": bpp_id,
                "bpp_uri": bpp_uri,
                "action": "select",
            },
            "message": {
                "order": {
                    "provider": {"id": provider["id"]},
                    "items": [
                        {
                            "id": item[0]["id"],
                            "quantity": {
                                "selected": {
                                    "measure": {
                                        "value": item[0]["quantity"]["selected"][
                                            "measure"
                                        ]["value"],
                                        "unit": item[0]["quantity"]["selected"][
                                            "measure"
                                        ]["unit"],
                                    }
                                }
                            },
                            "fulfillment_ids": [item[0]["fulfillment_ids"][0]],
                        }
                    ],
                    "fulfillments": [
                        {
                            "id": fulfillments[0]["id"],
                            "type": fulfillments[0]["type"],
                            "customer": {
                                "person": {
                                    "id": fulfillments[0]["customer"]["person"]["id"],
                                }
                            },
                            "agent": {
                                "person": {
                                    "id": fulfillments[0]["agent"]["person"]["id"]
                                },
                                "organization": {
                                    "creds": [
                                        {
                                            "id": fulfillments[0]["agent"][
                                                "organization"
                                            ]["creds"][0]["id"],
                                            "type": "ARN",
                                        },
                                    ]
                                },
                            },
                        }
                    ],
                    "xinput": {
                        "form": {"id": xinput["form"]["id"]},
                        "form_response": {
                            "submission_id": xinput["form_response"]["submission_id"]
                        },
                    },
                    "tags": [
                        {
                            "display": False,
                            "descriptor": {
                                "name": "BAP Terms of Engagement",
                                "code": "BAP_TERMS",
                            },
                            "list": [
                                {
                                    "descriptor": {
                                        "name": "Static Terms (Transaction Level)",
                                        "code": "STATIC_TERMS",
                                    },
                                    "value": "https://buyerapp.com/legal/ondc:fis14/static_terms?v=0.1",
                                },
                                {
                                    "descriptor": {
                                        "name": "Offline Contract",
                                        "code": "OFFLINE_CONTRACT",
                                    },
                                    "value": "true",
                                },
                            ],
                        }
                    ],
                }
            },
        }
        request_body_str = json.dumps(payload, separators=(",", ":"))
        auth_header = create_authorisation_header(request_body=request_body_str)

        headers = {
            "Content-Type": "application/json",
            "Authorization": auth_header,
            "X-Gateway-Authorization": os.getenv("SIGNED_UNIQUE_REQ_ID", ""),
            "X-Gateway-Subscriber-Id": os.getenv("SUBSCRIBER_ID"),
        }

        response = requests.post(
            f"{bpp_uri}/select", data=request_body_str, headers=headers
        )
        try:
            send_to_analytics(schema_type="select", req_body=payload)
        except Exception as e:
            logger.error(f"Observability logging failed: {str(e)}", exc_info=True)
        return Response(
            {
                "status_code": response.status_code,
                "response": response.json() if response.content else {},
            },
            status=status.HTTP_200_OK,
        )


class LumpsumEsignFormSubmission(APIView):

    def post(self, request, *args, **kwargs):
        transaction_id = request.data.get("transaction_id")
        bpp_id = request.data.get("bpp_id")
        bpp_uri = request.data.get("bpp_uri")
        message_id_select = request.data.get("message_id_select")
        message_id = request.data.get("message_id")

        if not all([transaction_id, bpp_id, bpp_uri, message_id_select]):
            return Response(
                {"error": "Required all Fields"}, status=status.HTTP_400_BAD_REQUEST
            )

        obj = (
            OnStatus.objects.filter(transaction__transaction_id=transaction_id)
            .order_by("-timestamp")
            .first()
        )

        if not message_id:
            message_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat(sep="T", timespec="milliseconds") + "Z"

        try:
            provider = obj.payload["message"]["order"]["provider"]
            item = obj.payload["message"]["order"]["items"]
            xinput = obj.payload["message"]["order"]["xinput"]
            fulfillments = obj.payload["message"]["order"]["fulfillments"]
        except (KeyError, TypeError) as e:
            return Response(
                {"error": f"Missing key in payload: {e}"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        payload = {
            "context": {
                "location": {"country": {"code": "IND"}, "city": {"code": "*"}},
                "domain": "ONDC:FIS14",
                "timestamp": timestamp,
                "bap_id": BAP_ID,
                "bap_uri": BAP_URI,
                "transaction_id": transaction_id,
                "message_id": message_id,
                "version": "2.0.0",
                "ttl": "PT10M",
                "bpp_id": bpp_id,
                "bpp_uri": bpp_uri,
                "action": "select",
            },
            "message": {
                "order": {
                    "provider": {"id": provider["id"]},
                    "items": [
                        {
                            "id": item[0]["id"],
                            "quantity": {
                                "selected": {
                                    "measure": {
                                        "value": item[0]["quantity"]["selected"][
                                            "measure"
                                        ]["value"],
                                        "unit": item[0]["quantity"]["selected"][
                                            "measure"
                                        ]["unit"],
                                    }
                                }
                            },
                            "fulfillment_ids": [item[0]["fulfillment_ids"][0]],
                        }
                    ],
                    "fulfillments": [
                        {
                            "id": fulfillments[0]["id"],
                            "type": fulfillments[0]["type"],
                            "customer": {
                                "person": {
                                    "id": fulfillments[0]["customer"]["person"]["id"],
                                }
                            },
                            "agent": {
                                "person": {
                                    "id": fulfillments[0]["agent"]["person"]["id"]
                                },
                                "organization": {
                                    "creds": [
                                        {
                                            "id": fulfillments[0]["agent"][
                                                "organization"
                                            ]["creds"][0]["id"],
                                            "type": "ARN",
                                        },
                                    ]
                                },
                            },
                        }
                    ],
                    "xinput": {
                        "form": {"id": xinput["form"]["id"]},
                        "form_response": {
                            "submission_id": xinput["form_response"]["submission_id"]
                        },
                    },
                    "tags": [
                        {
                            "display": False,
                            "descriptor": {
                                "name": "BAP Terms of Engagement",
                                "code": "BAP_TERMS",
                            },
                            "list": [
                                {
                                    "descriptor": {
                                        "name": "Static Terms (Transaction Level)",
                                        "code": "STATIC_TERMS",
                                    },
                                    "value": "https://buyerapp.com/legal/ondc:fis14/static_terms?v=0.1",
                                },
                                {
                                    "descriptor": {
                                        "name": "Offline Contract",
                                        "code": "OFFLINE_CONTRACT",
                                    },
                                    "value": "true",
                                },
                            ],
                        }
                    ],
                }
            },
        }
        request_body_str = json.dumps(payload, separators=(",", ":"))
        auth_header = create_authorisation_header(request_body=request_body_str)

        headers = {
            "Content-Type": "application/json",
            "Authorization": auth_header,
            "X-Gateway-Authorization": os.getenv("SIGNED_UNIQUE_REQ_ID", ""),
            "X-Gateway-Subscriber-Id": os.getenv("SUBSCRIBER_ID"),
        }

        response = requests.post(
            f"{bpp_uri}/select", data=request_body_str, headers=headers
        )
        try:
            send_to_analytics(schema_type="select", req_body=payload)
        except Exception as e:
            logger.error(f"Observability logging failed: {str(e)}", exc_info=True)
        return Response(
            {
                "status_code": response.status_code,
                "response": response.json() if response.content else {},
            },
            status=status.HTTP_200_OK,
        )


# Lumpsum Investment (Existing Folio - Investor selects/enters a folio)


class LumpsumExistingFolioInit(APIView):
    def post(self, request, *args, **kwargs):
        transaction_id = request.data.get("transaction_id")
        bpp_id = request.data.get("bpp_id")
        bpp_uri = request.data.get("bpp_uri")
        message_id = request.data.get("message_id")
        phone = request.data.get("phone")
        ifsc = request.data.get("ifsc")
        account_number = request.data.get("account_number")
        name = request.data.get("name")
        payment_mode = request.data.get("payment_mode")

        if not all([transaction_id, bpp_id, bpp_uri, message_id]):
            return Response(
                {"error": "Missing transaction_id, bpp_id, or bpp_uri"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        obj = get_object_or_404(
            SelectSIP,
            payload__context__bpp_id=bpp_id,
            payload__context__bpp_uri=bpp_uri,
            transaction__transaction_id=transaction_id,
        )

        if not message_id:
            message_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat(sep="T", timespec="milliseconds") + "Z"

        try:
            provider = obj.payload["message"]["order"]["provider"]
            item = obj.payload["message"]["order"]["items"]
            fulfillments = obj.payload["message"]["order"]["fulfillments"]
            payments = obj.payload["message"]["order"]["payments"]
        except KeyError as e:
            return Response(
                {"error": f"Missing key in payload: {e}"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        except TypeError:
            return Response(
                {"error": "Invalid payload structure (possibly None or wrong type)"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        payload = {
            "context": {
                "location": {"country": {"code": "IND"}, "city": {"code": "*"}},
                "domain": "ONDC:FIS14",
                "timestamp": timestamp,
                "bap_id": BAP_ID,
                "bap_uri": BAP_URI,
                "transaction_id": transaction_id,
                "message_id": message_id,
                "version": "2.0.0",
                "ttl": "PT10M",
                "bpp_id": bpp_id,
                "bpp_uri": bpp_uri,
                "action": "init",
            },
            "message": {
                "order": {
                    "provider": {"id": provider["id"]},
                    "items": [
                        {
                            "id": item[0]["id"],
                            "quantity": {
                                "selected": {
                                    "measure": {
                                        "value": item[0]["quantity"]["selected"][
                                            "measure"
                                        ]["value"],
                                        "unit": item[0]["quantity"]["selected"][
                                            "measure"
                                        ]["unit"],
                                    }
                                }
                            },
                            "fulfillment_ids": [item[0]["fulfillment_ids"][0]],
                        }
                    ],
                    "fulfillments": [
                        {
                            "id": fulfillments[0]["id"],
                            "type": fulfillments[0]["type"],
                            "customer": {
                                "person": {
                                    "id": fulfillments[0]["customer"]["person"]["id"],
                                    "creds": [
                                        {
                                            "id": fulfillments[0]["tags"][1]["list"][0][
                                                "value"
                                            ],
                                            "type": "FOLIO",
                                        },
                                        {
                                            "id": get_client_ip(request),
                                            "type": "IP_ADDRESS",
                                        },
                                    ],
                                },
                                "contact": {"phone": phone},
                            },
                            "agent": {
                                "person": {
                                    "id": fulfillments[0]["agent"]["person"]["id"],
                                },
                                "organization": {
                                    "creds": [
                                        {
                                            "id": fulfillments[0]["agent"][
                                                "organization"
                                            ]["creds"][0]["id"],
                                            "type": "ARN",
                                        },
                                    ]
                                },
                            },
                        }
                    ],
                    "payments": [
                        {
                            "collected_by": payments[0]["collected_by"],
                            "params": {
                                "amount": item[0]["quantity"]["selected"]["measure"][
                                    "value"
                                ],
                                "currency": item[0]["quantity"]["selected"]["measure"][
                                    "unit"
                                ],
                                "source_bank_code": ifsc,
                                "source_bank_account_number": account_number,
                                "source_bank_account_name": name,
                            },
                            "type": payments[0]["type"],
                            "tags": [
                                {
                                    "descriptor": {
                                        "name": "Payment Method",
                                        "code": "PAYMENT_METHOD",
                                    },
                                    "list": [
                                        {
                                            "descriptor": {"code": "MODE"},
                                            "value": payment_mode,
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                    "tags": [
                        {
                            "display": False,
                            "descriptor": {
                                "name": "BAP Terms of Engagement",
                                "code": "BAP_TERMS",
                            },
                            "list": [
                                {
                                    "descriptor": {
                                        "name": "Static Terms (Transaction Level)",
                                        "code": "STATIC_TERMS",
                                    },
                                    "value": "https://buyerapp.com/legal/ondc:fis14/static_terms?v=0.1",
                                },
                                {
                                    "descriptor": {
                                        "name": "Offline Contract",
                                        "code": "OFFLINE_CONTRACT",
                                    },
                                    "value": "true",
                                },
                            ],
                        }
                    ],
                }
            },
        }

        request_body_str = json.dumps(payload, separators=(",", ":"))
        auth_header = create_authorisation_header(request_body=request_body_str)

        headers = {
            "Content-Type": "application/json",
            "Authorization": auth_header,
            "X-Gateway-Authorization": os.getenv("SIGNED_UNIQUE_REQ_ID", ""),
            "X-Gateway-Subscriber-Id": os.getenv("SUBSCRIBER_ID"),
        }

        response = requests.post(
            f"{bpp_uri}/init", data=request_body_str, headers=headers
        )
        try:
            send_to_analytics(schema_type="init", req_body=payload)
        except Exception as e:
            logger.error(f"Observability logging failed: {str(e)}", exc_info=True)
        return Response(
            {
                "status_code": response.status_code,
                "response": response.json() if response.content else {},
            },
            status=status.HTTP_200_OK,
        )


class LumpConfirmExisting(APIView):
    def post(self, request, *args, **kwargs):
        transaction_id = request.data.get("transaction_id")
        bpp_id = request.data.get("bpp_id")
        bpp_uri = request.data.get("bpp_uri")
        message_id = request.data.get("message_id")

        if not all([transaction_id, bpp_id, bpp_uri, message_id]):
            return Response(
                {"error": "Required all Fields"}, status=status.HTTP_400_BAD_REQUEST
            )

        obj = get_object_or_404(
            OnInitSIP,
            payload__context__bpp_id=bpp_id,
            payload__context__bpp_uri=bpp_uri,
            transaction__transaction_id=transaction_id,
        )

        if not message_id:
            message_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat(sep="T", timespec="milliseconds") + "Z"

        try:
            id = obj.payload["message"]["order"]["id"]
            provider = obj.payload["message"]["order"]["provider"]
            item = obj.payload["message"]["order"]["items"]
            fulfillments = obj.payload["message"]["order"]["fulfillments"]
            payments = obj.payload["message"]["order"]["payments"]
            url = payments[0]["url"]
        except (KeyError, TypeError) as e:
            return Response(
                {"error": f"Missing key in payload: {e}"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        payload = {
            "context": {
                "location": {"country": {"code": "IND"}, "city": {"code": "*"}},
                "domain": "ONDC:FIS14",
                "timestamp": timestamp,
                "bap_id": BAP_ID,
                "bap_uri": BAP_URI,
                "transaction_id": transaction_id,
                "message_id": message_id,
                "version": "2.0.0",
                "ttl": "PT10M",
                "bpp_id": bpp_id,
                "bpp_uri": bpp_uri,
                "action": "confirm",
            },
            "message": {
                "order": {
                    "id": id,
                    "provider": {"id": provider["id"]},
                    "items": [
                        {
                            "id": item[0]["id"],
                            "quantity": {
                                "selected": {
                                    "measure": {
                                        "value": item[0]["quantity"]["selected"][
                                            "measure"
                                        ]["value"],
                                        "unit": item[0]["quantity"]["selected"][
                                            "measure"
                                        ]["unit"],
                                    }
                                }
                            },
                            "fulfillment_ids": [item[0]["fulfillment_ids"][0]],
                            "payment_ids": [item[0]["payment_ids"][0]],
                        }
                    ],
                    "fulfillments": [
                        {
                            "id": fulfillments[0]["id"],
                            "type": fulfillments[0]["type"],
                            "customer": {
                                "person": {
                                    "id": fulfillments[0]["customer"]["person"]["id"],
                                    "creds": [
                                        {
                                            "id": fulfillments[0]["customer"]["person"][
                                                "creds"
                                            ][0]["id"],
                                            "type": "FOLIO",
                                        },
                                        {
                                            "id": fulfillments[0]["customer"]["person"][
                                                "creds"
                                            ][1]["id"],
                                            "type": "IP_ADDRESS",
                                        },
                                    ],
                                },
                                "contact": {
                                    "phone": fulfillments[0]["customer"]["contact"][
                                        "phone"
                                    ]
                                },
                            },
                            "agent": {
                                "person": {
                                    "id": fulfillments[0]["agent"]["person"]["id"]
                                },
                                "organization": {
                                    "creds": [
                                        {
                                            "id": fulfillments[0]["agent"][
                                                "organization"
                                            ]["creds"][0]["id"],
                                            "type": "ARN",
                                        },
                                    ]
                                },
                            },
                        }
                    ],
                    "payments": [
                        {
                            "id": payments[0]["id"],
                            "collected_by": payments[0]["collected_by"],
                            "status": payments[0]["status"],
                            "params": {
                                "amount": payments[0]["params"]["amount"],
                                "currency": payments[0]["params"]["currency"],
                                "source_bank_code": payments[0]["params"][
                                    "source_bank_code"
                                ],
                                "source_bank_account_number": payments[0]["params"][
                                    "source_bank_account_number"
                                ],
                                "source_bank_account_name": payments[0]["params"][
                                    "source_bank_account_name"
                                ],
                            },
                            "type": payments[0]["type"],
                            "tags": [
                                {
                                    "descriptor": {
                                        "name": "Payment Method",
                                        "code": "PAYMENT_METHOD",
                                    },
                                    "list": [
                                        {
                                            "descriptor": {"code": "MODE"},
                                            "value": payments[0]["tags"][0]["list"][0][
                                                "value"
                                            ],
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                    "tags": [
                        {
                            "display": False,
                            "descriptor": {
                                "name": "BAP Terms of Engagement",
                                "code": "BAP_TERMS",
                            },
                            "list": [
                                {
                                    "descriptor": {
                                        "name": "Static Terms (Transaction Level)",
                                        "code": "STATIC_TERMS",
                                    },
                                    "value": "https://buyerapp.com/legal/ondc:fis14/static_terms?v=0.1",
                                },
                                {
                                    "descriptor": {
                                        "name": "Offline Contract",
                                        "code": "OFFLINE_CONTRACT",
                                    },
                                    "value": "true",
                                },
                            ],
                        },
                        {
                            "display": False,
                            "descriptor": {
                                "name": "BPP Terms of Engagement",
                                "code": "BPP_TERMS",
                            },
                            "list": [
                                {
                                    "descriptor": {
                                        "name": "Static Terms (Transaction Level)",
                                        "code": "STATIC_TERMS",
                                    },
                                    "value": "https://sellerapp.com/legal/ondc:fis14/static_terms?v=0.1",
                                },
                                {
                                    "descriptor": {
                                        "name": "Offline Contract",
                                        "code": "OFFLINE_CONTRACT",
                                    },
                                    "value": "true",
                                },
                            ],
                        },
                    ],
                }
            },
        }
        request_body_str = json.dumps(payload, separators=(",", ":"))
        auth_header = create_authorisation_header(request_body=request_body_str)

        headers = {
            "Content-Type": "application/json",
            "Authorization": auth_header,
            "X-Gateway-Authorization": os.getenv("SIGNED_UNIQUE_REQ_ID", ""),
            "X-Gateway-Subscriber-Id": os.getenv("SUBSCRIBER_ID"),
        }

        response = requests.post(
            f"{bpp_uri}/confirm", data=request_body_str, headers=headers
        )
        try:
            send_to_analytics(schema_type="confirm", req_body=payload)
        except Exception as e:
            logger.error(f"Observability logging failed: {str(e)}", exc_info=True)
        return Response(
            {
                "status_code": response.status_code,
                "response": response.json() if response.content else {},
            },
            status=status.HTTP_200_OK,
        )


# Lumpsum Payment Retry


class LumpRetryInit(APIView):
    def post(self, request, *args, **kwargs):
        transaction_id = request.data.get("transaction_id")
        bpp_id = request.data.get("bpp_id")
        bpp_uri = request.data.get("bpp_uri")
        message_id = request.data.get("message_id")
        phone = request.data.get("phone", "1234567890")
        ifsc = request.data.get("ifsc", "HDFC0000089")
        account_number = request.data.get("account_number", "004701563111")
        name = request.data.get("name", "harish gupta")

        if not all([transaction_id, bpp_id, bpp_uri, message_id]):
            return Response(
                {"error": "Missing transaction_id, bpp_id, or bpp_uri"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        obj = get_object_or_404(
            SelectSIP,
            payload__context__bpp_id=bpp_id,
            payload__context__bpp_uri=bpp_uri,
            transaction__transaction_id=transaction_id,
        )

        message_id_init = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat(sep="T", timespec="milliseconds") + "Z"

        try:
            provider = obj.payload["message"]["order"]["provider"]
            item = obj.payload["message"]["order"]["items"]
            fulfillments = obj.payload["message"]["order"]["fulfillments"]
            payment = obj.payload["message"]["order"]["payments"]
        except KeyError as e:
            return Response(
                {"error": f"Missing key in payload: {e}"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        except TypeError:
            return Response(
                {"error": "Invalid payload structure (possibly None or wrong type)"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        payload = {
            "context": {
                "location": {"country": {"code": "IND"}, "city": {"code": "*"}},
                "domain": "ONDC:FIS14",
                "timestamp": timestamp,
                "bap_id": BAP_ID,
                "bap_uri": BAP_URI,
                "transaction_id": transaction_id,
                "message_id": message_id_init,
                "version": "2.0.0",
                "ttl": "PT10M",
                "bpp_id": bpp_id,
                "bpp_uri": bpp_uri,
                "action": "init",
            },
            "message": {
                "order": {
                    "provider": {"id": provider["id"]},
                    "items": [
                        {
                            "id": item[0]["id"],
                            "quantity": {
                                "selected": {
                                    "measure": {
                                        "value": item[0]["quantity"]["selected"][
                                            "measure"
                                        ]["value"],
                                        "unit": item[0]["quantity"]["selected"][
                                            "measure"
                                        ]["unit"],
                                    }
                                }
                            },
                            "fulfillment_ids": [
                                obj.payload["message"]["order"]["quote"]["breakup"][0][
                                    "item"
                                ]["fulfillment_ids"][0]
                            ],
                        }
                    ],
                    "fulfillments": [
                        {
                            "id": fulfillments[0]["id"],
                            "type": fulfillments[0]["type"],
                            "customer": {
                                "person": {
                                    "id": fulfillments[0]["customer"]["person"]["id"],
                                    "creds": [
                                        {
                                            "id": fulfillments[0]["tags"][1]["list"][0][
                                                "value"
                                            ],
                                            "type": "FOLIO",
                                        },
                                        {
                                            "id": get_client_ip(request),
                                            "type": "IP_ADDRESS",
                                        },
                                    ],
                                },
                                "contact": {"phone": phone},
                            },
                            "agent": {
                                "person": {
                                    "id": fulfillments[0]["agent"]["person"]["id"]
                                },
                                "organization": {
                                    "creds": [
                                        {
                                            "id": fulfillments[0]["agent"][
                                                "organization"
                                            ]["creds"][0]["id"],
                                            "type": "ARN",
                                        },
                                    ]
                                },
                            },
                        }
                    ],
                    "payments": [
                        {
                            "collected_by": payment[0]["collected_by"],
                            "params": {
                                "amount": item[0]["quantity"]["selected"]["measure"][
                                    "value"
                                ],
                                "currency": item[0]["quantity"]["selected"]["measure"][
                                    "unit"
                                ],
                                "source_bank_code": ifsc,
                                "source_bank_account_number": account_number,
                                "source_bank_account_name": name,
                            },
                            "type": payment[0]["type"],
                            "tags": [
                                {
                                    "descriptor": {
                                        "name": "Payment Method",
                                        "code": "PAYMENT_METHOD",
                                    },
                                    "list": [
                                        {
                                            "descriptor": {"code": "MODE"},
                                            "value": payment[0]["tags"][0]["list"][0][
                                                "value"
                                            ],
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                    "tags": [
                        {
                            "display": False,
                            "descriptor": {
                                "name": "BAP Terms of Engagement",
                                "code": "BAP_TERMS",
                            },
                            "list": [
                                {
                                    "descriptor": {
                                        "name": "Static Terms (Transaction Level)",
                                        "code": "STATIC_TERMS",
                                    },
                                    "value": "https://buyerapp.com/legal/ondc:fis14/static_terms?v=0.1",
                                },
                                {
                                    "descriptor": {
                                        "name": "Offline Contract",
                                        "code": "OFFLINE_CONTRACT",
                                    },
                                    "value": "true",
                                },
                            ],
                        }
                    ],
                }
            },
        }
        request_body_str = json.dumps(payload, separators=(",", ":"))
        auth_header = create_authorisation_header(request_body=request_body_str)

        headers = {
            "Content-Type": "application/json",
            "Authorization": auth_header,
            "X-Gateway-Authorization": os.getenv("SIGNED_UNIQUE_REQ_ID", ""),
            "X-Gateway-Subscriber-Id": os.getenv("SUBSCRIBER_ID"),
        }

        response = requests.post(
            f"{bpp_uri}/init", data=request_body_str, headers=headers
        )
        try:
            send_to_analytics(schema_type="init", req_body=payload)
        except Exception as e:
            logger.error(f"Observability logging failed: {str(e)}", exc_info=True)
        return Response(
            {
                "status_code": response.status_code,
                "response": response.json() if response.content else {},
            },
            status=status.HTTP_200_OK,
        )


class LumpRetryConfirm(APIView):
    def post(self, request, *args, **kwargs):
        transaction_id = request.data.get("transaction_id")
        bpp_id = request.data.get("bpp_id")
        bpp_uri = request.data.get("bpp_uri")
        message_id = request.data.get("message_id")

        if not all([transaction_id, bpp_id, bpp_uri, message_id]):
            return Response(
                {"error": "Missing transaction_id, bpp_id, or bpp_uri"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        obj = get_object_or_404(
            OnInitSIP,
            payload__context__bpp_id=bpp_id,
            payload__context__bpp_uri=bpp_uri,
            transaction__transaction_id=transaction_id,
        )

        message_id_confirm = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat(sep="T", timespec="milliseconds") + "Z"

        try:
            provider = obj.payload["message"]["order"]["provider"]
            item = obj.payload["message"]["order"]["items"]
            fulfillments = obj.payload["message"]["order"]["fulfillments"]
            payment = obj.payload["message"]["order"]["payments"]
        except KeyError as e:
            return Response(
                {"error": f"Missing key in payload: {e}"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        except TypeError:
            return Response(
                {"error": "Invalid payload structure (possibly None or wrong type)"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        payload = {
            "context": {
                "location": {"country": {"code": "IND"}, "city": {"code": "*"}},
                "domain": "ONDC:FIS14",
                "timestamp": timestamp,
                "bap_id": BAP_ID,
                "bap_uri": BAP_URI,
                "transaction_id": transaction_id,
                "message_id": message_id_confirm,
                "version": "2.0.0",
                "ttl": "PT10M",
                "bpp_id": bpp_id,
                "bpp_uri": bpp_uri,
                "action": "confirm",
            },
            "message": {
                "order": {
                    "id": obj.payload["message"]["order"]["id"],
                    "provider": {"id": provider["id"]},
                    "items": [
                        {
                            "id": item[0]["id"],
                            "quantity": {
                                "selected": {
                                    "measure": {
                                        "value": item[0]["quantity"]["selected"][
                                            "measure"
                                        ]["value"],
                                        "unit": item[0]["quantity"]["selected"][
                                            "measure"
                                        ]["unit"],
                                    }
                                }
                            },
                            "fulfillment_ids": [
                                obj.payload["message"]["order"]["quote"]["breakup"][0][
                                    "item"
                                ]["fulfillment_ids"][0]
                            ],
                            "payment_ids": [item[0]["payment_ids"][0]],
                        }
                    ],
                    "fulfillments": [
                        {
                            "id": fulfillments[0]["id"],
                            "type": fulfillments[0]["type"],
                            "customer": {
                                "person": {
                                    "id": fulfillments[0]["customer"]["person"]["id"],
                                    "creds": [
                                        {
                                            "id": fulfillments[0]["customer"]["person"][
                                                "creds"
                                            ][0]["id"],
                                            "type": "FOLIO",
                                        },
                                        {
                                            "id": fulfillments[0]["customer"]["person"][
                                                "creds"
                                            ][1]["id"],
                                            "type": "IP_ADDRESS",
                                        },
                                    ],
                                },
                                "contact": {
                                    "phone": fulfillments[0]["customer"]["contact"][
                                        "phone"
                                    ]
                                },
                            },
                            "agent": {
                                "person": {
                                    "id": fulfillments[0]["agent"]["person"]["id"]
                                },
                                "organization": {
                                    "creds": [
                                        {
                                            "id": fulfillments[0]["agent"][
                                                "organization"
                                            ]["creds"][0]["id"],
                                            "type": "ARN",
                                        },
                                    ]
                                },
                            },
                        }
                    ],
                    "payments": [
                        {
                            "id": payment[0]["id"],
                            "collected_by": payment[0]["collected_by"],
                            "status": payment[0]["status"],
                            "params": {
                                "amount": payment[0]["params"]["amount"],
                                "currency": payment[0]["params"]["currency"],
                                "source_bank_code": payment[0]["params"][
                                    "source_bank_code"
                                ],
                                "source_bank_account_number": payment[0]["params"][
                                    "source_bank_account_number"
                                ],
                                "source_bank_account_name": payment[0]["params"][
                                    "source_bank_account_name"
                                ],
                            },
                            "type": payment[0]["type"],
                            "tags": [
                                {
                                    "descriptor": {
                                        "name": "Payment Method",
                                        "code": "PAYMENT_METHOD",
                                    },
                                    "list": [
                                        {
                                            "descriptor": {"code": "MODE"},
                                            "value": payment[0]["tags"][0]["list"][0][
                                                "value"
                                            ],
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                    "tags": [
                        {
                            "display": False,
                            "descriptor": {
                                "name": "BAP Terms of Engagement",
                                "code": "BAP_TERMS",
                            },
                            "list": [
                                {
                                    "descriptor": {
                                        "name": "Static Terms (Transaction Level)",
                                        "code": "STATIC_TERMS",
                                    },
                                    "value": "https://buyerapp.com/legal/ondc:fis14/static_terms?v=0.1",
                                },
                                {
                                    "descriptor": {
                                        "name": "Offline Contract",
                                        "code": "OFFLINE_CONTRACT",
                                    },
                                    "value": "true",
                                },
                            ],
                        },
                        {
                            "display": False,
                            "descriptor": {
                                "name": "BPP Terms of Engagement",
                                "code": "BPP_TERMS",
                            },
                            "list": [
                                {
                                    "descriptor": {
                                        "name": "Static Terms (Transaction Level)",
                                        "code": "STATIC_TERMS",
                                    },
                                    "value": "https://sellerapp.com/legal/ondc:fis14/static_terms?v=0.1",
                                },
                                {
                                    "descriptor": {
                                        "name": "Offline Contract",
                                        "code": "OFFLINE_CONTRACT",
                                    },
                                    "value": "true",
                                },
                            ],
                        },
                    ],
                }
            },
        }
        request_body_str = json.dumps(payload, separators=(",", ":"))
        auth_header = create_authorisation_header(request_body=request_body_str)

        headers = {
            "Content-Type": "application/json",
            "Authorization": auth_header,
            "X-Gateway-Authorization": os.getenv("SIGNED_UNIQUE_REQ_ID", ""),
            "X-Gateway-Subscriber-Id": os.getenv("SUBSCRIBER_ID"),
        }

        response = requests.post(
            f"{bpp_uri}/confirm", data=request_body_str, headers=headers
        )
        try:
            send_to_analytics(schema_type="confirm", req_body=payload)
        except Exception as e:
            logger.error(f"Observability logging failed: {str(e)}", exc_info=True)

        return Response(
            {
                "status_code": response.status_code,
                "response": response.json() if response.content else {},
            },
            status=status.HTTP_200_OK,
        )


class LumpRetryUpdate(APIView):
    def post(self, request, *args, **kwargs):
        transaction_id = request.data.get("transaction_id")
        bpp_id = request.data.get("bpp_id")
        bpp_uri = request.data.get("bpp_uri")
        message_id = request.data.get("message_id")

        if not all([transaction_id, bpp_id, bpp_uri]):
            return Response(
                {"error": "Missing transaction_id, bpp_id, or bpp_uri"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        obj = get_object_or_404(
            OnUpdate,
            payload__context__bpp_id=bpp_id,
            payload__context__bpp_uri=bpp_uri,
            transaction__transaction_id=transaction_id,
        )

        if not message_id:
            message_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat(sep="T", timespec="milliseconds") + "Z"

        try:
            provider = obj.payload["message"]["order"]["provider"]
            item = obj.payload["message"]["order"]["items"]
            fulfillments = obj.payload["message"]["order"]["fulfillments"]
            payment = obj.payload["message"]["order"]["payments"]
        except KeyError as e:
            return Response(
                {"error": f"Missing key in payload: {e}"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        except TypeError:
            return Response(
                {"error": "Invalid payload structure (possibly None or wrong type)"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        payload = {
            "context": {
                "location": {"country": {"code": "IND"}, "city": {"code": "*"}},
                "domain": "ONDC:FIS14",
                "timestamp": timestamp,
                "bap_id": BAP_ID,
                "bap_uri": BAP_URI,
                "transaction_id": transaction_id,
                "message_id": message_id,
                "version": "2.0.0",
                "ttl": "PT10M",
                "bpp_id": bpp_id,
                "bpp_uri": bpp_uri,
                "action": "update",
            },
            "message": {
                "update_target": "order.payments",
                "order": {
                    "id": obj.payload["message"]["order"]["id"],
                    "payments": [
                        {
                            "collected_by": payment[0]["collected_by"],
                            "params": {
                                "amount": payment[0]["params"]["amount"],
                                "currency": payment[0]["params"]["currency"],
                                "source_bank_code": payment[0]["params"][
                                    "source_bank_code"
                                ],
                                "source_bank_account_number": payment[0]["params"][
                                    "source_bank_account_number"
                                ],
                                "source_bank_account_name": payment[0]["params"][
                                    "source_bank_account_name"
                                ],
                            },
                            "type": payment[0]["type"],
                            "tags": [
                                {
                                    "descriptor": {
                                        "name": "Payment Method",
                                        "code": "PAYMENT_METHOD",
                                    },
                                    "list": [
                                        {
                                            "descriptor": {"code": "MODE"},
                                            "value": payment[0]["tags"][0]["list"][0][
                                                "value"
                                            ],
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                },
            },
        }

        request_body_str = json.dumps(payload, separators=(",", ":"))
        auth_header = create_authorisation_header(request_body=request_body_str)

        headers = {
            "Content-Type": "application/json",
            "Authorization": auth_header,
            "X-Gateway-Authorization": os.getenv("SIGNED_UNIQUE_REQ_ID", ""),
            "X-Gateway-Subscriber-Id": os.getenv("SUBSCRIBER_ID"),
        }

        response = requests.post(
            f"{bpp_uri}/update", data=request_body_str, headers=headers
        )
        try:
            send_to_analytics(schema_type="update", req_body=payload)
        except Exception as e:
            logger.error(f"Observability logging failed: {str(e)}", exc_info=True)

        return Response(
            {
                "status_code": response.status_code,
                "response": response.json() if response.content else {},
            },
            status=status.HTTP_200_OK,
        )


# Redemption


class RedemptionSelect(APIView):
    def post(self, request, *args, **kwargs):
        transaction_id = request.data.get("transaction_id")
        bpp_id = request.data.get("bpp_id")
        bpp_uri = request.data.get("bpp_uri")
        message_id = request.data.get("message_id")
        preferred_type = "REDEMPTION"
        amount = request.data.get("amount")
        pan = request.data.get("pan")
        name = request.data.get("name")
        folio = request.data.get("folio")

        if not all([transaction_id, bpp_id, bpp_uri, preferred_type]):
            return Response(
                {"error": "Missing transaction_id, bpp_id, or bpp_uri"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        obj = get_object_or_404(
            FullOnSearch,
            payload__context__bpp_id=bpp_id,
            payload__context__bpp_uri=bpp_uri,
            transaction__transaction_id=transaction_id,
        )

        if not message_id:
            message_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat(sep="T", timespec="milliseconds") + "Z"
        print(obj.payload)

        # Get the first provider and item
        provider = obj.payload["message"]["catalog"]["providers"][0]
        catalog = obj.payload["message"]["catalog"]
        fulfillment_type_map = {
            f["id"]: f["type"] for f in provider.get("fulfillments", [])
        }
        matching_fulfillment_id = next(
            (
                fid
                for item in provider.get("items", [])
                for fid in item.get("fulfillment_ids", [])
                if fulfillment_type_map.get(fid) == preferred_type
            ),
            None,
        )
        matching_fulfillment = next(
            (f for f in provider["fulfillments"] if f.get("type") == preferred_type),
            None,
        )

        if not matching_fulfillment:
            return Response(
                {"error": f"No fulfillment with type '{preferred_type}' found."},
                status=status.HTTP_404_NOT_FOUND,
            )

        payload = {
            "context": {
                "location": {"country": {"code": "IND"}, "city": {"code": "*"}},
                "domain": "ONDC:FIS14",
                "timestamp": timestamp,
                "bap_id": BAP_ID,
                "bap_uri": BAP_URI,
                "transaction_id": transaction_id,
                "message_id": message_id,
                "version": "2.0.0",
                "ttl": "PT10M",
                "bpp_id": bpp_id,
                "bpp_uri": bpp_uri,
                "action": "select",
            },
            "message": {
                "order": {
                    "provider": {"id": provider["id"]},
                    "items": [
                        {
                            "id": provider["items"][0]["id"],
                            "quantity": {
                                "selected": {
                                    "measure": {"value": amount, "unit": "INR"}
                                }
                            },
                            "fulfillment_ids": [matching_fulfillment_id],
                        }
                    ],
                    "fulfillments": [
                        {
                            "id": matching_fulfillment["id"],
                            "type": matching_fulfillment["type"],
                            "customer": {
                                "person": {
                                    "id": "pan:" + pan,
                                    "creds": [{"id": folio, "type": "FOLIO"}],
                                }
                            },
                            "agent": {
                                "person": {"id": os.getenv("EUIN")},
                                "organization": {
                                    "creds": [
                                        {"id": os.getenv("ARN"), "type": "ARN"},
                                    ]
                                },
                            },
                        }
                    ],
                    "tags": [
                        {
                            "display": False,
                            "descriptor": {
                                "name": "BAP Terms of Engagement",
                                "code": "BAP_TERMS",
                            },
                            "list": [
                                {
                                    "descriptor": {
                                        "name": "Static Terms (Transaction Level)",
                                        "code": "STATIC_TERMS",
                                    },
                                    "value": "https://buyerapp.com/legal/ondc:fis14/static_terms?v=0.1",
                                },
                                {
                                    "descriptor": {
                                        "name": "Offline Contract",
                                        "code": "OFFLINE_CONTRACT",
                                    },
                                    "value": "true",
                                },
                            ],
                        }
                    ],
                }
            },
        }
        request_body_str = json.dumps(payload, separators=(",", ":"))
        auth_header = create_authorisation_header(request_body=request_body_str)

        headers = {
            "Content-Type": "application/json",
            "Authorization": auth_header,
            "X-Gateway-Authorization": os.getenv("SIGNED_UNIQUE_REQ_ID", ""),
            "X-Gateway-Subscriber-Id": os.getenv("SUBSCRIBER_ID"),
        }

        response = requests.post(
            f"{bpp_uri}/select", data=request_body_str, headers=headers
        )
        try:
            send_to_analytics(schema_type="select", req_body=payload)
        except Exception as e:
            logger.error(f"Observability logging failed: {str(e)}", exc_info=True)

        return Response(
            {
                "status_code": response.status_code,
                "response": response.json() if response.content else {},
            },
            status=status.HTTP_200_OK,
        )


class RedemptionInit(APIView):
    def post(self, request, *args, **kwargs):

        transaction_id = request.data.get("transaction_id")
        bpp_id = request.data.get("bpp_id")
        bpp_uri = request.data.get("bpp_uri")
        message_id_select = request.data.get("message_id_select")
        phone = request.data.get("phone")
        ifsc = request.data.get("ifsc")
        account_number = request.data.get("account_number")
        name = request.data.get("name")
        message_id = request.data.get("message_id")

        if not all([transaction_id, bpp_id, bpp_uri]):
            return Response(
                {"error": "Missing transaction_id, bpp_id, or bpp_uri"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        obj = get_object_or_404(
            SelectSIP,
            payload__context__bpp_id=bpp_id,
            payload__context__bpp_uri=bpp_uri,
            transaction__transaction_id=transaction_id,
        )

        if not message_id:
            message_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat(sep="T", timespec="milliseconds") + "Z"

        try:
            provider = obj.payload["message"]["order"]["provider"]
            item = obj.payload["message"]["order"]["items"]
            fulfillments = obj.payload["message"]["order"]["fulfillments"]
        except KeyError as e:
            return Response(
                {"error": f"Missing key in payload: {e}"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        except TypeError:
            return Response(
                {"error": "Invalid payload structure (possibly None or wrong type)"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        payload = {
            "context": {
                "location": {"country": {"code": "IND"}, "city": {"code": "*"}},
                "domain": "ONDC:FIS14",
                "timestamp": timestamp,
                "bap_id": BAP_ID,
                "bap_uri": BAP_URI,
                "transaction_id": transaction_id,
                "message_id": message_id,
                "version": "2.0.0",
                "ttl": "PT10M",
                "bpp_id": bpp_id,
                "bpp_uri": bpp_uri,
                "action": "init",
            },
            "message": {
                "order": {
                    "provider": {"id": provider["id"]},
                    "items": [
                        {
                            "id": item[0]["id"],
                            "quantity": {
                                "selected": {
                                    "measure": {
                                        "value": item[0]["quantity"]["selected"][
                                            "measure"
                                        ]["value"],
                                        "unit": item[0]["quantity"]["selected"][
                                            "measure"
                                        ]["unit"],
                                    }
                                }
                            },
                            "fulfillment_ids": [item[0]["fulfillment_ids"][0]],
                        }
                    ],
                    "fulfillments": [
                        {
                            "id": fulfillments[0]["id"],
                            "type": fulfillments[0]["type"],
                            "customer": {
                                "person": {
                                    "id": fulfillments[0]["customer"]["person"]["id"],
                                    "creds": [
                                        {
                                            "id": fulfillments[0]["customer"]["person"][
                                                "creds"
                                            ][0]["id"],
                                            "type": "FOLIO",
                                        },
                                        {
                                            "id": get_client_ip(request),
                                            "type": "IP_ADDRESS",
                                        },
                                    ],
                                },
                                "contact": {"phone": phone},
                            },
                            "agent": {
                                "person": {
                                    "id": fulfillments[0]["agent"]["person"]["id"]
                                },
                                "organization": {
                                    "creds": [
                                        {
                                            "id": fulfillments[0]["agent"][
                                                "organization"
                                            ]["creds"][0]["id"],
                                            "type": "ARN",
                                        },
                                    ]
                                },
                            },
                            "tags": [
                                {
                                    "descriptor": {
                                        "name": "Payout Bank Account",
                                        "code": "PAYOUT_BANK_ACCOUNT",
                                    },
                                    "list": [
                                        {
                                            "descriptor": {
                                                "name": "Identifier",
                                                "code": "IDENTIFIER",
                                            },
                                            "value": fulfillments[0]["tags"][1]["list"][
                                                0
                                            ]["value"],
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                    "tags": [
                        {
                            "display": False,
                            "descriptor": {
                                "name": "BAP Terms of Engagement",
                                "code": "BAP_TERMS",
                            },
                            "list": [
                                {
                                    "descriptor": {
                                        "name": "Static Terms (Transaction Level)",
                                        "code": "STATIC_TERMS",
                                    },
                                    "value": "https://buyerapp.com/legal/ondc:fis14/static_terms?v=0.1",
                                },
                                {
                                    "descriptor": {
                                        "name": "Offline Contract",
                                        "code": "OFFLINE_CONTRACT",
                                    },
                                    "value": "true",
                                },
                            ],
                        }
                    ],
                }
            },
        }

        # Send to gateway
        request_body_str = json.dumps(payload, separators=(",", ":"))
        auth_header = create_authorisation_header(request_body=request_body_str)

        headers = {
            "Content-Type": "application/json",
            "Authorization": auth_header,
            "X-Gateway-Authorization": os.getenv("SIGNED_UNIQUE_REQ_ID", ""),
            "X-Gateway-Subscriber-Id": os.getenv("SUBSCRIBER_ID"),
        }

        response = requests.post(
            f"{bpp_uri}/init", data=request_body_str, headers=headers
        )
        try:
            send_to_analytics(schema_type="init", req_body=payload)
        except Exception as e:
            logger.error(f"Observability logging failed: {str(e)}", exc_info=True)

        return Response(
            {
                "status_code": response.status_code,
                "response": response.json() if response.content else {},
            },
            status=status.HTTP_200_OK,
        )


class RedemptionConfirm(APIView):

    def post(self, request, *args, **kwargs):
        transaction_id = request.data.get("transaction_id")
        bpp_id = request.data.get("bpp_id")
        bpp_uri = request.data.get("bpp_uri")
        message_id = request.data.get("message_id")

        if not all([transaction_id, bpp_id, bpp_uri]):
            return Response(
                {"error": "Missing transaction_id, bpp_id, or bpp_uri"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        obj = get_object_or_404(
            OnInitSIP,
            payload__context__bpp_id=bpp_id,
            payload__context__bpp_uri=bpp_uri,
            transaction__transaction_id=transaction_id,
        )

        if not message_id:
            message_id = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat(sep="T", timespec="milliseconds") + "Z"

        try:
            provider = obj.payload["message"]["order"]["provider"]
            item = obj.payload["message"]["order"]["items"]
            fulfillments = obj.payload["message"]["order"]["fulfillments"]
        except KeyError as e:
            return Response(
                {"error": f"Missing key in payload: {e}"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        except TypeError:
            return Response(
                {"error": "Invalid payload structure (possibly None or wrong type)"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        payload = {
            "context": {
                "location": {"country": {"code": "IND"}, "city": {"code": "*"}},
                "domain": "ONDC:FIS14",
                "timestamp": timestamp,
                "bap_id": BAP_ID,
                "bap_uri": BAP_URI,
                "transaction_id": transaction_id,
                "message_id": message_id,
                "version": "2.0.0",
                "ttl": "PT10M",
                "bpp_id": bpp_id,
                "bpp_uri": bpp_uri,
                "action": "confirm",
            },
            "message": {
                "order": {
                    "id": obj.payload["message"]["order"]["id"],
                    "provider": {"id": provider["id"]},
                    "items": [
                        {
                            "id": item[0]["id"],
                            "quantity": {
                                "selected": {
                                    "measure": {
                                        "value": item[0]["quantity"]["selected"][
                                            "measure"
                                        ]["value"],
                                        "unit": item[0]["quantity"]["selected"][
                                            "measure"
                                        ]["unit"],
                                    }
                                }
                            },
                            "fulfillment_ids": [item[0]["fulfillment_ids"][0]],
                        }
                    ],
                    "fulfillments": [
                        {
                            "id": fulfillments[0]["id"],
                            "type": fulfillments[0]["type"],
                            "customer": {
                                "person": {
                                    "id": fulfillments[0]["customer"]["person"]["id"],
                                    "creds": [
                                        {
                                            "id": fulfillments[0]["customer"]["person"][
                                                "creds"
                                            ][0]["id"],
                                            "type": "FOLIO",
                                        },
                                        {
                                            "id": fulfillments[0]["customer"]["person"][
                                                "creds"
                                            ][1]["id"],
                                            "type": "IP_ADDRESS",
                                        },
                                    ],
                                },
                                "contact": {
                                    "phone": fulfillments[0]["customer"]["contact"][
                                        "phone"
                                    ]
                                },
                            },
                            "agent": {
                                "person": {
                                    "id": fulfillments[0]["agent"]["person"]["id"]
                                },
                                "organization": {
                                    "creds": [
                                        {
                                            "id": fulfillments[0]["agent"][
                                                "organization"
                                            ]["creds"][0]["id"],
                                            "type": "ARN",
                                        },
                                    ]
                                },
                            },
                            "tags": [
                                {
                                    "descriptor": {
                                        "name": "Payout Bank Account",
                                        "code": "PAYOUT_BANK_ACCOUNT",
                                    },
                                    "list": [
                                        {
                                            "descriptor": {
                                                "name": "Identifier",
                                                "code": "IDENTIFIER",
                                            },
                                            "value": fulfillments[0]["tags"][0]["list"][
                                                0
                                            ]["value"],
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                    "tags": [
                        {
                            "display": False,
                            "descriptor": {
                                "name": "BAP Terms of Engagement",
                                "code": "BAP_TERMS",
                            },
                            "list": [
                                {
                                    "descriptor": {
                                        "name": "Static Terms (Transaction Level)",
                                        "code": "STATIC_TERMS",
                                    },
                                    "value": "https://buyerapp.com/legal/ondc:fis14/static_terms?v=0.1",
                                },
                                {
                                    "descriptor": {
                                        "name": "Offline Contract",
                                        "code": "OFFLINE_CONTRACT",
                                    },
                                    "value": "true",
                                },
                            ],
                        },
                        {
                            "display": False,
                            "descriptor": {
                                "name": "BPP Terms of Engagement",
                                "code": "BPP_TERMS",
                            },
                            "list": [
                                {
                                    "descriptor": {
                                        "name": "Static Terms (Transaction Level)",
                                        "code": "STATIC_TERMS",
                                    },
                                    "value": "https://sellerapp.com/legal/ondc:fis14/static_terms?v=0.1",
                                },
                                {
                                    "descriptor": {
                                        "name": "Offline Contract",
                                        "code": "OFFLINE_CONTRACT",
                                    },
                                    "value": "true",
                                },
                            ],
                        },
                    ],
                }
            },
        }

        request_body_str = json.dumps(payload, separators=(",", ":"))
        auth_header = create_authorisation_header(request_body=request_body_str)

        headers = {
            "Content-Type": "application/json",
            "Authorization": auth_header,
            "X-Gateway-Authorization": os.getenv("SIGNED_UNIQUE_REQ_ID", ""),
            "X-Gateway-Subscriber-Id": os.getenv("SUBSCRIBER_ID"),
        }

        response = requests.post(
            f"{bpp_uri}/confirm", data=request_body_str, headers=headers
        )

        try:
            send_to_analytics(schema_type="confirm", req_body=payload)
        except Exception as e:
            logger.error(f"Observability logging failed: {str(e)}", exc_info=True)

        return Response(
            {
                "status_code": response.status_code,
                "response": response.json() if response.content else {},
            },
            status=status.HTTP_200_OK,
        )


# For testing Only

# views.py - Add this new view to orchestrate the complete flow

import asyncio
import logging
import time

from django.http import JsonResponse
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class CompleteSIPFlowView(APIView):
    """
    Orchestrates the complete SIP creation flow with a single API call
    """

    def post(self, request, *args, **kwargs):
        preferred_type = request.data.get("preferred_type", "SIP")

        try:
            # Step 1: Search
            search_result = self._execute_search()
            if not search_result["success"]:
                return Response(search_result, status=status.HTTP_400_BAD_REQUEST)

            transaction_id = search_result["transaction_id"]

            # Step 2: Wait for on_search callback (with timeout)
            on_search_data = self._wait_for_on_search(transaction_id)
            if not on_search_data:
                return Response(
                    {"success": False, "error": "Timeout waiting for search results"},
                    status=status.HTTP_408_REQUEST_TIMEOUT,
                )

            # Step 3: Select SIP
            select_result = self._execute_select(
                transaction_id,
                on_search_data["bpp_id"],
                on_search_data["bpp_uri"],
                preferred_type,
            )
            if not select_result["success"]:
                return Response(select_result, status=status.HTTP_400_BAD_REQUEST)

            # Step 4: Wait for on_select callback
            on_select_data = self._wait_for_on_select(transaction_id)
            if not on_select_data:
                return Response(
                    {
                        "success": False,
                        "error": "Timeout waiting for select confirmation",
                    },
                    status=status.HTTP_408_REQUEST_TIMEOUT,
                )

            # Step 5: Submit Form (if required)
            form_result = self._execute_form_submission(
                transaction_id, on_select_data["bpp_id"], on_select_data["bpp_uri"]
            )
            if not form_result["success"]:
                return Response(form_result, status=status.HTTP_400_BAD_REQUEST)

            # Step 6: Initialize
            init_result = self._execute_init(
                transaction_id,
                on_select_data["bpp_id"],
                on_select_data["bpp_uri"],
                form_result["message_id"],
            )
            if not init_result["success"]:
                return Response(init_result, status=status.HTTP_400_BAD_REQUEST)

            # Step 7: Wait for on_init callback
            on_init_data = self._wait_for_on_init(transaction_id)
            if not on_init_data:
                return Response(
                    {
                        "success": False,
                        "error": "Timeout waiting for init confirmation",
                    },
                    status=status.HTTP_408_REQUEST_TIMEOUT,
                )

            # Step 8: Confirm SIP
            confirm_result = self._execute_confirm(
                transaction_id,
                on_init_data["bpp_id"],
                on_init_data["bpp_uri"],
                on_init_data["message_id"],
            )
            if not confirm_result["success"]:
                return Response(confirm_result, status=status.HTTP_400_BAD_REQUEST)

            # Step 9: Wait for final on_confirm callback
            on_confirm_data = self._wait_for_on_confirm(transaction_id)
            if not on_confirm_data:
                return Response(
                    {
                        "success": False,
                        "error": "Timeout waiting for final confirmation",
                    },
                    status=status.HTTP_408_REQUEST_TIMEOUT,
                )

            return Response(
                {
                    "success": True,
                    "message": "SIP created successfully",
                    "transaction_id": transaction_id,
                    "order_details": on_confirm_data.get("order_details"),
                    "flow_summary": {
                        "search_completed": True,
                        "select_completed": True,
                        "form_submitted": True,
                        "init_completed": True,
                        "confirm_completed": True,
                    },
                },
                status=status.HTTP_200_OK,
            )

        except Exception as e:
            logger.error(f"Complete SIP flow failed: {str(e)}", exc_info=True)
            return Response(
                {"success": False, "error": f"Flow execution failed: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    def _execute_search(self):
        """Execute the search step"""
        try:
            transaction_id = str(uuid.uuid4())
            message_id = str(uuid.uuid4())
            timestamp = (
                datetime.utcnow().isoformat(sep="T", timespec="milliseconds") + "Z"
            )

            payload = {
                "context": {
                    "location": {"country": {"code": "IND"}, "city": {"code": "*"}},
                    "domain": "ONDC:FIS14",
                    "timestamp": timestamp,
                    "bap_id": BAP_ID,
                    "bap_uri": BAP_URI,
                    "transaction_id": transaction_id,
                    "message_id": message_id,
                    "version": "2.0.0",
                    "ttl": "PT10M",
                    "action": "search",
                },
                "message": {
                    "intent": {
                        "category": {"descriptor": {"code": "MUTUAL_FUNDS"}},
                        "fulfillment": {
                            "agent": {
                                "organization": {
                                    "creds": [{"id": "ARN-125784", "type": "ARN"}]
                                }
                            }
                        },
                        "tags": [
                            {
                                "display": False,
                                "descriptor": {
                                    "name": "BAP Terms of Engagement",
                                    "code": "BAP_TERMS",
                                },
                                "list": [
                                    {
                                        "descriptor": {
                                            "name": "Static Terms (Transaction Level)",
                                            "code": "STATIC_TERMS",
                                        },
                                        "value": "https://buyerapp.com/legal/ondc:fis14/static_terms?v=0.1",
                                    },
                                    {
                                        "descriptor": {
                                            "name": "Offline Contract",
                                            "code": "OFFLINE_CONTRACT",
                                        },
                                        "value": "true",
                                    },
                                ],
                            }
                        ],
                    }
                },
            }

            # Store transaction
            transaction, _ = Transaction.objects.get_or_create(
                transaction_id=transaction_id
            )
            Message.objects.create(
                transaction=transaction,
                message_id=message_id,
                action="search",
                timestamp=parse_datetime(timestamp),
                payload=payload,
            )

            # Send request
            request_body_str = json.dumps(payload, separators=(",", ":"))
            auth_header = create_authorisation_header(request_body=request_body_str)

            headers = {
                "Content-Type": "application/json",
                "Authorization": auth_header,
                "X-Gateway-Authorization": os.getenv("SIGNED_UNIQUE_REQ_ID", ""),
                "X-Gateway-Subscriber-Id": os.getenv("SUBSCRIBER_ID"),
            }

            response = requests.post(
                "https://preprod.gateway.ondc.org/search",
                data=request_body_str,
                headers=headers,
            )

            if response.status_code == 200:
                return {
                    "success": True,
                    "transaction_id": transaction_id,
                    "message_id": message_id,
                }
            else:
                return {
                    "success": False,
                    "error": f"Search request failed with status {response.status_code}",
                }

        except Exception as e:
            return {"success": False, "error": str(e)}

    def _wait_for_on_search(self, transaction_id, timeout=30):
        """Wait for on_search callback with timeout"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                transaction = Transaction.objects.get(transaction_id=transaction_id)
                search_entry = FullOnSearch.objects.filter(
                    transaction=transaction
                ).first()
                if search_entry:
                    payload = search_entry.payload
                    return {
                        "bpp_id": payload["context"]["bpp_id"],
                        "bpp_uri": payload["context"]["bpp_uri"],
                        "payload": payload,
                    }
            except:
                pass
            time.sleep(2)  # Wait 2 seconds before checking again
        return None

    def _execute_select(self, transaction_id, bpp_id, bpp_uri, preferred_type):
        """Execute the select step"""
        try:
            obj = FullOnSearch.objects.get(
                payload__context__bpp_id=bpp_id,
                payload__context__bpp_uri=bpp_uri,
                transaction__transaction_id=transaction_id,
            )

            message_id = str(uuid.uuid4())
            timestamp = (
                datetime.utcnow().isoformat(sep="T", timespec="milliseconds") + "Z"
            )

            provider = obj.payload["message"]["catalog"]["providers"][0]
            matching_fulfillment = next(
                (
                    f
                    for f in provider["fulfillments"]
                    if f.get("type") == preferred_type
                ),
                None,
            )

            if not matching_fulfillment:
                return {
                    "success": False,
                    "error": f"No fulfillment with type {preferred_type} found",
                }

            # Build select payload (similar to your existing SIPCreationView)
            payload = {
                "context": {
                    "location": {"country": {"code": "IND"}, "city": {"code": "*"}},
                    "domain": "ONDC:FIS14",
                    "timestamp": timestamp,
                    "bap_id": BAP_ID,
                    "bap_uri": BAP_URI,
                    "transaction_id": transaction_id,
                    "message_id": message_id,
                    "version": "2.0.0",
                    "ttl": "PT10M",
                    "bpp_id": bpp_id,
                    "bpp_uri": bpp_uri,
                    "action": "select",
                },
                "message": {
                    "order": {
                        "provider": {"id": provider["id"]},
                        "items": [
                            {
                                "id": provider["items"][0]["id"],
                                "quantity": {
                                    "selected": {
                                        "measure": {"value": "3000", "unit": "INR"}
                                    }
                                },
                            }
                        ],
                        "fulfillments": [
                            {
                                "id": matching_fulfillment["id"],
                                "type": matching_fulfillment["type"],
                                "customer": {"person": {"id": "pan:arrpp7771n"}},
                                "agent": {
                                    "person": {"id": "euin:E52432"},
                                    "organization": {
                                        "creds": [
                                            {"id": "ARN-124567", "type": "ARN"},
                                            {
                                                "id": "ARN-123456",
                                                "type": "SUB_BROKER_ARN",
                                            },
                                        ]
                                    },
                                },
                                "stops": [
                                    {
                                        "time": {
                                            "schedule": {
                                                "frequency": matching_fulfillment[
                                                    "tags"
                                                ][0]["list"][0]["value"]
                                            }
                                        }
                                    }
                                ],
                            }
                        ],
                        "tags": [
                            {
                                "display": False,
                                "descriptor": {
                                    "name": "BAP Terms of Engagement",
                                    "code": "BAP_TERMS",
                                },
                                "list": [
                                    {
                                        "descriptor": {
                                            "name": "Static Terms (Transaction Level)",
                                            "code": "STATIC_TERMS",
                                        },
                                        "value": "https://buyerapp.com/legal/ondc:fis14/static_terms?v=0.1",
                                    },
                                    {
                                        "descriptor": {
                                            "name": "Offline Contract",
                                            "code": "OFFLINE_CONTRACT",
                                        },
                                        "value": "true",
                                    },
                                ],
                            }
                        ],
                    }
                },
            }

            # Store and send
            transaction = Transaction.objects.get(transaction_id=transaction_id)
            Message.objects.create(
                transaction=transaction,
                message_id=message_id,
                action="select",
                timestamp=parse_datetime(timestamp),
                payload=payload,
            )

            request_body_str = json.dumps(payload, separators=(",", ":"))
            auth_header = create_authorisation_header(request_body=request_body_str)

            headers = {
                "Content-Type": "application/json",
                "Authorization": auth_header,
                "X-Gateway-Authorization": os.getenv("SIGNED_UNIQUE_REQ_ID", ""),
                "X-Gateway-Subscriber-Id": os.getenv("SUBSCRIBER_ID"),
            }

            response = requests.post(
                f"{bpp_uri}/select", data=request_body_str, headers=headers
            )

            if response.status_code == 200:
                return {"success": True, "message_id": message_id}
            else:
                return {
                    "success": False,
                    "error": f"Select request failed with status {response.status_code}",
                }

        except Exception as e:
            return {"success": False, "error": str(e)}

    def _wait_for_on_select(self, transaction_id, timeout=30):
        """Wait for on_select callback"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                transaction = Transaction.objects.get(transaction_id=transaction_id)
                select_entry = SelectSIP.objects.filter(transaction=transaction).first()
                if select_entry:
                    payload = select_entry.payload
                    return {
                        "bpp_id": payload["context"]["bpp_id"],
                        "bpp_uri": payload["context"]["bpp_uri"],
                        "message_id": payload["context"]["message_id"],
                        "payload": payload,
                    }
            except:
                pass
            time.sleep(2)
        return None

    def _execute_form_submission(self, transaction_id, bpp_id, bpp_uri):
        """Execute form submission step"""
        try:
            obj = SelectSIP.objects.get(
                payload__context__bpp_id=bpp_id,
                payload__context__bpp_uri=bpp_uri,
                transaction__transaction_id=transaction_id,
            )

            message_id = str(uuid.uuid4())
            timestamp = (
                datetime.utcnow().isoformat(sep="T", timespec="milliseconds") + "Z"
            )

            # Extract form URL
            xinput = obj.payload["message"]["order"]["xinput"]
            url = xinput["form"]["url"]

            # Submit KYC data
            user_kyc_data = {
                "pan": "ABCDE1234F",
                "dob": "1990-01-01",
                "email": "user@example.com",
                "name": "Ravi Kumar",
                "gender": "Male",
                "marital_status": "Married",
                "occupation": "Salaried",
                "source_of_wealth": "Business",
                "income_range": "1L to 5L",
                "cob": "India",
                "pob": "Kochi",
                "political_exposure": "no_exposure",
                "india_tax_residency_status": "resident",
                "mode_of_holding": "single",
                "ca_line": "hfjfk jifl jffj",
            }

            res = requests.post(url, json=user_kyc_data)
            if res.status_code != 200:
                return {
                    "success": False,
                    "error": f"Form submission failed with status {res.status_code}",
                }

            resp_json = res.json()
            submission_id = resp_json.get("submission_id")
            if not submission_id:
                return {
                    "success": False,
                    "error": "Submission ID missing from form response",
                }

            # Store submission ID
            SubmissionID.objects.create(
                transaction=obj.transaction,
                submission_id=submission_id,
                message_id=message_id,
                timestamp=timestamp,
            )

            # Continue with the rest of form submission logic...
            # (Include the full payload construction from your FormSubmisssion view)

            return {
                "success": True,
                "message_id": message_id,
                "submission_id": submission_id,
            }

        except Exception as e:
            return {"success": False, "error": str(e)}

    def _execute_init(self, transaction_id, bpp_id, bpp_uri, message_id):
        """Execute init step"""
        # Implementation similar to your INIT view
        try:
            # Your existing INIT logic here
            return {"success": True, "message_id": str(uuid.uuid4())}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _wait_for_on_init(self, transaction_id, timeout=30):
        """Wait for on_init callback"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                transaction = Transaction.objects.get(transaction_id=transaction_id)
                init_entry = OnInitSIP.objects.filter(transaction=transaction).first()
                if init_entry:
                    payload = init_entry.payload
                    return {
                        "bpp_id": payload["context"]["bpp_id"],
                        "bpp_uri": payload["context"]["bpp_uri"],
                        "message_id": payload["context"]["message_id"],
                        "payload": payload,
                    }
            except:
                pass
            time.sleep(2)
        return None

    def _execute_confirm(self, transaction_id, bpp_id, bpp_uri, message_id):
        """Execute confirm step"""
        # Implementation similar to your ConfirmSIP view
        try:
            # Your existing ConfirmSIP logic here
            return {"success": True, "message_id": str(uuid.uuid4())}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _wait_for_on_confirm(self, transaction_id, timeout=30):
        """Wait for on_confirm callback"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                transaction = Transaction.objects.get(transaction_id=transaction_id)
                confirm_entry = OnConfirm.objects.filter(
                    transaction=transaction
                ).first()
                if confirm_entry:
                    return {
                        "order_details": confirm_entry.payload.get("message", {}).get(
                            "order", {}
                        ),
                        "payload": confirm_entry.payload,
                    }
            except:
                pass
            time.sleep(2)
        return None


class OnSelectDataView(APIView):
    def post(self, request, *args, **kwargs):
        try:
            if isinstance(request.data, str):
                data = json.loads(request.data)
            else:
                data = request.data

            transaction_id = data.get("transaction_id")
            message_id = data.get("message_id")

            if not all([transaction_id, message_id]):
                return Response(
                    {"error": "Missing transaction_id,message_id"},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            queryset = SelectSIP.objects.filter(
                transaction__transaction_id=transaction_id, message_id=message_id
            )

            obj = get_object_or_404(queryset)

            return Response(
                {
                    "status": "success",
                    "message": "on_select processed successfully",
                    "data": obj.payload,
                },
                status=status.HTTP_200_OK,
            )

        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)


class OnInitDataView(APIView):
    """
    View to handle the ONDC on_init callback
    """

    def post(self, request, *args, **kwargs):
        try:
            transaction_id = request.data.get("transaction_id")
            message_id = request.data.get("message_id")
            if not all([transaction_id, message_id]):
                return Response(
                    {"error": "Missing transaction_id or message_id"},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            obj = get_object_or_404(
                OnInitSIP,
                transaction__transaction_id=transaction_id,
                message_id=message_id,
            )
            return Response(
                {
                    "status": "success",
                    "message": "on_init processed successfully",
                    "data": obj.payload,
                },
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            return Response(
                {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class OnConfirmDataView(APIView):
    """
    View to handle the ONDC on_confirm callback
    """

    def post(self, request, *args, **kwargs):
        try:
            transaction_id = request.data.get("transaction_id")
            message_id = request.data.get("message_id")

            if not all([transaction_id, message_id]):
                return Response(
                    {"error": "Missing transaction_id, bpp_id, bpp_uri or message_id"},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            obj = get_object_or_404(
                OnConfirm,
                transaction__transaction_id=transaction_id,
                message_id=message_id,
            )

            return Response(
                {
                    "status": "success",
                    "message": "on_confirm processed successfully",
                    "data": obj.payload,
                },
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            return Response(
                {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class OnStatusDataView(APIView):
    """
    View to handle the ONDC on_status callback
    """

    def post(self, request, *args, **kwargs):
        try:
            transaction_id = request.data.get("transaction_id")
            message_id = request.data.get("message_id")

            if not all([transaction_id]):
                return Response(
                    {"error": "Missing transaction_id or message_id"},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            obj = (
                OnStatus.objects.filter(transaction__transaction_id=transaction_id)
                .order_by("-timestamp")
                .first()
            )

            return Response(
                {
                    "status": "success",
                    "message": "on_status processed successfully",
                    "data": obj.payload,
                },
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            return Response(
                {"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class OnUpdateDataView(APIView):
    """
    View to handle the ONDC on_update callback
    """

    def post(self, request, *args, **kwargs):
        try:
            transaction_id = request.data.get("transaction_id")

            if not all([transaction_id]):
                return Response(
                    {"error": "Missing transaction_id or message_id"},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            obj = (
                OnUpdate.objects.filter(transaction__transaction_id=transaction_id)
                .order_by("-timestamp")
                .first()
            )

            return Response(
                {
                    "status": "success",
                    "message": "on_update processed successfully",
                    "data": obj.payload,
                },
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)


class OnStatusListPan(APIView):

    def post(self, request, *args, **kwargs):
        try:
            pan = request.data.get("pan")
            if not pan:
                return Response(
                    {"error": "Missing pan"}, status=status.HTTP_400_BAD_REQUEST
                )

            obj = OnStatus.objects.filter(pan=pan).order_by("-timestamp").first()
            if not obj:
                return Response(
                    {"error": "No records found for the given PAN"},
                    status=status.HTTP_404_NOT_FOUND,
                )
            return Response(
                {
                    "status": "success",
                    "message": "Status processed successfully",
                    "data": obj.payload,
                },
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)


class OnCancelDataView(APIView):

    def post(self, request, *args, **kwargs):
        try:
            transaction_id = request.data.get("transaction_id")

            if not all([transaction_id]):
                return Response(
                    {"error": "Missing transaction_id or message_id"},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            obj = get_object_or_404(
                OnCancel,
                transaction__transaction_id=transaction_id,
            )

            return Response(
                {
                    "status": "success",
                    "message": "on_cancel processed successfully",
                    "data": obj.payload,
                },
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)
