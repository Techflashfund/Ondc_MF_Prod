from django.db import models


class Transaction(models.Model):
    transaction_id = models.CharField(max_length=100, unique=True)
    created_at = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=50, blank=True, null=True)

    def __str__(self):
        return self.transaction_id


class Message(models.Model):
    transaction = models.ForeignKey(
        Transaction, on_delete=models.CASCADE, related_name="messages"
    )
    message_id = models.CharField(max_length=100, unique=True)
    action = models.CharField(max_length=50)
    payload = models.JSONField()
    timestamp = models.DateTimeField()

    def __str__(self):
        return f"{self.transaction.transaction_id} - {self.message_id}"


class FullOnSearch(models.Model):
    transaction = models.ForeignKey(
        Transaction, on_delete=models.CASCADE, related_name="full_on_searchs"
    )
    message_id = models.CharField(max_length=100)
    payload = models.JSONField()
    timestamp = models.DateTimeField()
    created_at = models.DateTimeField(auto_now_add=True)
    isin = models.CharField(max_length=50, null=True, blank=True)

    def __str__(self):
        return f"{self.transaction.transaction_id} - {self.message_id}"


class Scheme(models.Model):
    full_on_search = models.ForeignKey(
        FullOnSearch, 
        on_delete=models.CASCADE, 
        related_name="schemes"
    )
    scheme_id = models.CharField(max_length=100, db_index=True)
    name = models.CharField(max_length=255)
    category_ids = models.JSONField()
    parent_item_id = models.CharField(max_length=100, null=True, blank=True)
    fulfillment_ids = models.JSONField(null=True, blank=True)
    tags = models.JSONField(null=True, blank=True)
    isin = models.CharField(max_length=50, null=True, blank=True, db_index=True)
    payload = models.JSONField(null=True, blank=True)  # <-- full raw scheme data

    provider_id = models.CharField(max_length=100, null=True, blank=True)
    item_id = models.CharField(max_length=100, null=True, blank=True)
    matching_fulfillment = models.JSONField(null=True, blank=True)

    def __str__(self):
        return f"{self.name} ({self.isin})"
    

from django.db import models
from django.db.models import JSONField

import uuid

class ONDCTransaction(models.Model):
    """Main transaction context from ONDC"""
    transaction_id = models.CharField(max_length=100, unique=True, db_index=True)
    message_id = models.CharField(max_length=100)
    bap_id = models.CharField(max_length=200)
    bap_uri = models.URLField()
    bpp_id = models.CharField(max_length=200)
    bpp_uri = models.URLField()
    domain = models.CharField(max_length=50, default="ONDC:FIS14")
    version = models.CharField(max_length=10, default="2.0.0")
    action = models.CharField(max_length=50)
    country_code = models.CharField(max_length=3, default="IND")
    city_code = models.CharField(max_length=10, default="*")
    timestamp = models.DateTimeField()
    ttl = models.CharField(max_length=20)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = 'ondc_transactions'
        indexes = [
            models.Index(fields=['bpp_id', 'transaction_id']),
            models.Index(fields=['timestamp']),
        ]

class MutualFundProvider(models.Model):
    """AMC/Provider information"""
    provider_id = models.CharField(max_length=100, db_index=True)
    name = models.CharField(max_length=200)
    bpp_id = models.CharField(max_length=200, db_index=True)  # To link with seller
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'mutual_fund_providers'
        unique_together = ['provider_id', 'bpp_id']
        indexes = [
            models.Index(fields=['bpp_id', 'is_active']),
        ]

class SchemeCategory(models.Model):
    """Hierarchical categories for mutual fund schemes"""
    category_id = models.CharField(max_length=50)
    provider = models.ForeignKey(MutualFundProvider, on_delete=models.CASCADE, related_name='categories')
    name = models.CharField(max_length=200)
    code = models.CharField(max_length=100)
    parent_category = models.ForeignKey('self', null=True, blank=True, on_delete=models.CASCADE)
    level = models.IntegerField(default=0)  # 0=root, 1=child, etc.
    
    class Meta:
        db_table = 'scheme_categories'
        unique_together = ['category_id', 'provider']

class MutualFundScheme(models.Model):
    """Main scheme information"""
    STATUS_CHOICES = [
        ('active', 'Active'),
        ('inactive', 'Inactive'),
        ('closed', 'Closed'),
    ]
    
    scheme_id = models.CharField(max_length=100)
    provider = models.ForeignKey(MutualFundProvider, on_delete=models.CASCADE, related_name='schemes')
    transaction = models.ForeignKey(ONDCTransaction, on_delete=models.CASCADE, related_name='schemes')
    
    name = models.CharField(max_length=300)
    code = models.CharField(max_length=50, default="SCHEME")
    categories = models.ManyToManyField(SchemeCategory, related_name='schemes')
    
    # Scheme Information
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='active')
    lockin_period_days = models.IntegerField(null=True, blank=True)
    nfo_start_date = models.DateField(null=True, blank=True)
    nfo_end_date = models.DateField(null=True, blank=True)
    nfo_allotment_date = models.DateField(null=True, blank=True)
    nfo_reopen_date = models.DateField(null=True, blank=True)
    entry_load = models.CharField(max_length=100, default="no entry load")
    exit_load = models.CharField(max_length=100, null=True, blank=True)
    offer_documents_url = models.URLField(null=True, blank=True)
    
    is_matched = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'mutual_fund_schemes'
        unique_together = ['scheme_id', 'provider']
        indexes = [
            models.Index(fields=['provider', 'status']),
            models.Index(fields=['nfo_start_date', 'nfo_end_date']),
        ]

class SchemePlan(models.Model):
    """Individual plans under a scheme (Regular/Direct, Growth/Dividend)"""
    PLAN_CHOICES = [
        ('REGULAR', 'Regular'),
        ('DIRECT', 'Direct'),
    ]
    
    OPTION_CHOICES = [
        ('GROWTH', 'Growth'),
        ('IDCW', 'IDCW'),
        ('DIVIDEND', 'Dividend'),
    ]
    
    IDCW_OPTION_CHOICES = [
        ('PAYOUT', 'Payout'),
        ('REINVESTMENT', 'Reinvestment'),
    ]
    
    plan_id = models.CharField(max_length=100)
    scheme = models.ForeignKey(MutualFundScheme, on_delete=models.CASCADE, related_name='plans')
    
    name = models.CharField(max_length=300)
    code = models.CharField(max_length=50, default="SCHEME_PLAN")
    
    # Plan Identifiers
    isin = models.CharField(max_length=20, null=True, blank=True)
    rta_identifier = models.CharField(max_length=20, null=True, blank=True)
    amfi_identifier = models.CharField(max_length=20, null=True, blank=True)
    
    # Plan Options
    plan_type = models.CharField(max_length=20, choices=PLAN_CHOICES)
    option_type = models.CharField(max_length=20, choices=OPTION_CHOICES)
    idcw_option = models.CharField(max_length=20, choices=IDCW_OPTION_CHOICES, null=True, blank=True)
    
    consumer_tnc_url = models.URLField(null=True, blank=True)
    is_matched = models.BooleanField(default=False)
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'scheme_plans'
        unique_together = ['plan_id', 'scheme']
        indexes = [
            models.Index(fields=['scheme', 'plan_type', 'option_type']),
            models.Index(fields=['isin']),
        ]

class FulfillmentOption(models.Model):
    """Investment/Redemption options (Lumpsum, SIP, SWP, etc.)"""
    FULFILLMENT_TYPES = [
        ('LUMPSUM', 'Lump Sum'),
        ('SIP', 'Systematic Investment Plan'),
        ('STP', 'Systematic Transfer Plan'),
        ('SWP', 'Systematic Withdrawal Plan'),
        ('REDEMPTION', 'Redemption'),
    ]
    
    fulfillment_id = models.CharField(max_length=100)
    plan = models.ForeignKey(SchemePlan, on_delete=models.CASCADE, related_name='fulfillment_options')
    
    fulfillment_type = models.CharField(max_length=20, choices=FULFILLMENT_TYPES)
    
    # Thresholds
    amount_min = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    amount_max = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    amount_multiples = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    
    units_min = models.DecimalField(max_digits=12, decimal_places=4, null=True, blank=True)
    units_max = models.DecimalField(max_digits=12, decimal_places=4, null=True, blank=True)
    units_multiples = models.DecimalField(max_digits=12, decimal_places=4, null=True, blank=True)
    
    # For SIP/STP/SWP
    frequency = models.CharField(max_length=10, null=True, blank=True)  # P1M, P1D, etc.
    frequency_dates = models.TextField(null=True, blank=True)  # Comma-separated dates
    instalments_count_min = models.IntegerField(null=True, blank=True)
    instalments_count_max = models.IntegerField(null=True, blank=True)
    cumulative_amount_min = models.DecimalField(max_digits=15, decimal_places=2, null=True, blank=True)
    
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = 'fulfillment_options'
        unique_together = ['fulfillment_id', 'plan']
        indexes = [
            models.Index(fields=['plan', 'fulfillment_type']),
        ]

class BPPTerms(models.Model):
    """BPP Terms and conditions"""
    transaction = models.OneToOneField(ONDCTransaction, on_delete=models.CASCADE, related_name='bpp_terms')
    static_terms_url = models.URLField(null=True, blank=True)
    offline_contract = models.BooleanField(default=False)
    additional_terms = JSONField(default=dict, blank=True)
    
    class Meta:
        db_table = 'bpp_terms'

# Service class for data processing
class ONDCMutualFundService:
    """Service class to handle ONDC mutual fund data processing"""
    
    @classmethod
    def process_ondc_response(cls, ondc_data):
        """
        Process complete ONDC response and store in database
        Returns: (transaction, created_schemes_count)
        """
        context = ondc_data['context']
        message = ondc_data['message']
        
        # Create transaction record
        transaction = ONDCTransaction.objects.create(
            transaction_id=context['transaction_id'],
            message_id=context['message_id'],
            bap_id=context['bap_id'],
            bap_uri=context['bap_uri'],
            bpp_id=context['bpp_id'],
            bpp_uri=context['bpp_uri'],
            domain=context['domain'],
            version=context['version'],
            action=context['action'],
            country_code=context['location']['country']['code'],
            city_code=context['location']['city']['code'],
            timestamp=context['timestamp'],
            ttl=context['ttl']
        )
        
        # Process BPP Terms
        if 'tags' in message['catalog']:
            bpp_terms_data = cls._extract_bpp_terms(message['catalog']['tags'])
            if bpp_terms_data:
                BPPTerms.objects.create(
                    transaction=transaction,
                    **bpp_terms_data
                )
        
        schemes_created = 0
        
        # Process providers and their schemes
        for provider_data in message['catalog']['providers']:
            provider = cls._create_or_update_provider(provider_data, context['bpp_id'])
            
            # Process categories
            categories_map = cls._process_categories(provider_data.get('categories', []), provider)
            
            # Process schemes and plans
            schemes_created += cls._process_schemes(
                provider_data.get('items', []), 
                provider, 
                transaction, 
                categories_map,
                provider_data.get('fulfillments', [])
            )
        
        return transaction, schemes_created
    
    @classmethod
    def _create_or_update_provider(cls, provider_data, bpp_id):
        """Create or update provider"""
        provider, created = MutualFundProvider.objects.get_or_create(
            provider_id=provider_data['id'],
            bpp_id=bpp_id,
            defaults={
                'name': provider_data['descriptor']['name'],
                'is_active': True
            }
        )
        if not created:
            provider.name = provider_data['descriptor']['name']
            provider.is_active = True
            provider.save()
        
        return provider
    
    @classmethod
    def _process_categories(cls, categories_data, provider):
        """Process hierarchical categories"""
        categories_map = {}
        
        # Sort by hierarchy (parents first)
        sorted_categories = sorted(categories_data, key=lambda x: 0 if 'parent_category_id' not in x else 1)
        
        for cat_data in sorted_categories:
            parent = None
            level = 0
            
            if 'parent_category_id' in cat_data:
                parent = categories_map.get(cat_data['parent_category_id'])
                level = parent.level + 1 if parent else 0
            
            category, created = SchemeCategory.objects.get_or_create(
                category_id=cat_data['id'],
                provider=provider,
                defaults={
                    'name': cat_data['descriptor']['name'],
                    'code': cat_data['descriptor']['code'],
                    'parent_category': parent,
                    'level': level
                }
            )
            
            categories_map[cat_data['id']] = category
        
        return categories_map
    
    @classmethod
    def _process_schemes(cls, items_data, provider, transaction, categories_map, fulfillments_data):
        """Process schemes and their plans"""
        schemes_created = 0
        fulfillments_map = {f['id']: f for f in fulfillments_data}
        
        # Separate schemes and plans
        schemes = [item for item in items_data if item['descriptor']['code'] == 'SCHEME']
        plans = [item for item in items_data if item['descriptor']['code'] == 'SCHEME_PLAN']
        
        for scheme_data in schemes:
            # Create scheme
            scheme = cls._create_scheme(scheme_data, provider, transaction, categories_map)
            schemes_created += 1
            
            # Process plans for this scheme
            scheme_plans = [p for p in plans if p.get('parent_item_id') == scheme_data['id']]
            
            for plan_data in scheme_plans:
                plan = cls._create_plan(plan_data, scheme)
                
                # Process fulfillment options for this plan
                if 'fulfillment_ids' in plan_data:
                    cls._create_fulfillment_options(plan_data['fulfillment_ids'], plan, fulfillments_map)
        
        return schemes_created
    
    @classmethod
    def _create_scheme(cls, scheme_data, provider, transaction, categories_map):
        """Create individual scheme"""
        # Extract scheme information from tags
        scheme_info = cls._extract_scheme_info(scheme_data.get('tags', []))
        
        scheme = MutualFundScheme.objects.create(
            scheme_id=scheme_data['id'],
            provider=provider,
            transaction=transaction,
            name=scheme_data['descriptor']['name'],
            code=scheme_data['descriptor']['code'],
            is_matched=scheme_data.get('matched', False),
            **scheme_info
        )
        
        # Add categories
        for cat_id in scheme_data.get('category_ids', []):
            if cat_id in categories_map:
                scheme.categories.add(categories_map[cat_id])
        
        return scheme
    
    @classmethod
    def _create_plan(cls, plan_data, scheme):
        """Create scheme plan"""
        # Extract plan information from tags
        plan_info = cls._extract_plan_info(plan_data.get('tags', []))
        
        plan = SchemePlan.objects.create(
            plan_id=plan_data['id'],
            scheme=scheme,
            name=plan_data['descriptor']['name'],
            code=plan_data['descriptor']['code'],
            is_matched=plan_data.get('matched', False),
            **plan_info
        )
        
        return plan
    
    @classmethod
    def _create_fulfillment_options(cls, fulfillment_ids, plan, fulfillments_map):
        """Create fulfillment options for a plan"""
        for fulfillment_id in fulfillment_ids:
            if fulfillment_id in fulfillments_map:
                fulfillment_data = fulfillments_map[fulfillment_id]
                thresholds = cls._extract_thresholds(fulfillment_data.get('tags', []))
                
                FulfillmentOption.objects.create(
                    fulfillment_id=fulfillment_id,
                    plan=plan,
                    fulfillment_type=fulfillment_data['type'],
                    **thresholds
                )
    
    @classmethod
    def _extract_scheme_info(cls, tags):
        """Extract scheme information from tags"""
        info = {}
        
        for tag in tags:
            if tag['descriptor']['code'] == 'SCHEME_INFORMATION':
                for item in tag['list']:
                    code = item['descriptor']['code']
                    value = item['value']
                    
                    if code == 'STATUS':
                        info['status'] = value
                    elif code == 'LOCKIN_PERIOD_IN_DAYS':
                        info['lockin_period_days'] = int(value) if value else None
                    elif code == 'NFO_START_DATE':
                        info['nfo_start_date'] = value
                    elif code == 'NFO_END_DATE':
                        info['nfo_end_date'] = value
                    elif code == 'NFO_ALLOTMENT_DATE':
                        info['nfo_allotment_date'] = value
                    elif code == 'NFO_REOPEN_DATE':
                        info['nfo_reopen_date'] = value
                    elif code == 'ENTRY_LOAD':
                        info['entry_load'] = value
                    elif code == 'EXIT_LOAD':
                        info['exit_load'] = value
                    elif code == 'OFFER_DOCUMENTS':
                        info['offer_documents_url'] = value
        
        return info
    
    @classmethod
    def _extract_plan_info(cls, tags):
        """Extract plan information from tags"""
        info = {
            'plan_type': 'REGULAR',
            'option_type': 'GROWTH'
        }
        
        for tag in tags:
            if tag['descriptor']['code'] == 'PLAN_IDENTIFIERS':
                for item in tag['list']:
                    code = item['descriptor']['code']
                    if code == 'ISIN':
                        info['isin'] = item['value']
                    elif code == 'RTA_IDENTIFIER':
                        info['rta_identifier'] = item['value']
                    elif code == 'AMFI_IDENTIFIER':
                        info['amfi_identifier'] = item['value']
            
            elif tag['descriptor']['code'] == 'PLAN_OPTIONS':
                for item in tag['list']:
                    code = item['descriptor']['code']
                    if code == 'PLAN':
                        info['plan_type'] = item['value']
                    elif code == 'OPTION':
                        info['option_type'] = item['value']
                    elif code == 'IDCW_OPTION':
                        info['idcw_option'] = item['value']
            
            elif tag['descriptor']['code'] == 'PLAN_INFORMATION':
                for item in tag['list']:
                    if item['descriptor']['code'] == 'CONSUMER_TNC':
                        info['consumer_tnc_url'] = item['value']
        
        return info
    
    @classmethod
    def _extract_thresholds(cls, tags):
        """Extract threshold information from fulfillment tags"""
        thresholds = {}
        
        for tag in tags:
            if tag['descriptor']['code'] == 'THRESHOLDS':
                for item in tag['list']:
                    code = item['descriptor']['code']
                    value = item['value']
                    
                    # Convert numeric values
                    if code in ['AMOUNT_MIN', 'AMOUNT_MAX', 'AMOUNT_MULTIPLES', 'CUMULATIVE_AMOUNT_MIN']:
                        thresholds[code.lower()] = float(value) if value else None
                    elif code in ['UNITS_MIN', 'UNITS_MAX', 'UNITS_MULTIPLES']:
                        thresholds[code.lower()] = float(value) if value else None
                    elif code in ['INSTALMENTS_COUNT_MIN', 'INSTALMENTS_COUNT_MAX']:
                        thresholds[code.lower()] = int(value) if value else None
                    elif code == 'FREQUENCY':
                        thresholds['frequency'] = value
                    elif code == 'FREQUENCY_DATES':
                        thresholds['frequency_dates'] = value
        
        return thresholds
    
    @classmethod
    def _extract_bpp_terms(cls, tags):
        """Extract BPP terms from catalog tags"""
        terms = {}
        
        for tag in tags:
            if tag['descriptor']['code'] == 'BPP_TERMS':
                for item in tag['list']:
                    code = item['descriptor']['code']
                    if code == 'STATIC_TERMS':
                        terms['static_terms_url'] = item['value']
                    elif code == 'OFFLINE_CONTRACT':
                        terms['offline_contract'] = item['value'].lower() == 'true'
        
        return terms if terms else None

# Usage Example:
"""
# To process ONDC response:
transaction, schemes_count = ONDCMutualFundService.process_ondc_response(ondc_json_data)
print(f"Created transaction {transaction.transaction_id} with {schemes_count} schemes")

# Query examples:
# Get all active schemes for a provider
active_schemes = MutualFundScheme.objects.filter(
    provider__bpp_id='api.sellerapp.com',
    status='active'
).select_related('provider').prefetch_related('plans', 'categories')

# Get SIP options for a specific plan
sip_options = FulfillmentOption.objects.filter(
    plan__isin='IN123214324',
    fulfillment_type='SIP'
)

# Get schemes by category
mid_cap_schemes = MutualFundScheme.objects.filter(
    categories__code='OPEN_ENDED_EQUITY_MIDCAP'
)
"""


class SelectSIP(models.Model):
    transaction = models.ForeignKey(
        Transaction, on_delete=models.CASCADE, related_name="full_on_selects"
    )
    message_id = models.CharField(max_length=100)
    payload = models.JSONField()
    timestamp = models.DateTimeField()

    def __str__(self):
        return f"{self.transaction.transaction_id} - {self.message_id}"


class SubmissionID(models.Model):
    transaction = models.ForeignKey(Transaction, on_delete=models.CASCADE)
    message_id = models.CharField(max_length=100)
    submission_id = models.CharField(max_length=100)
    timestamp = models.DateTimeField()

    def __str__(self):
        return f"{self.transaction.transaction_id} - {self.submission_id}"


class OnInitSIP(models.Model):
    transaction = models.ForeignKey(
        Transaction, on_delete=models.CASCADE, related_name="full_on_init"
    )
    message_id = models.CharField(max_length=100)
    payload = models.JSONField()
    timestamp = models.DateTimeField()

    def __str__(self):
        return f"{self.transaction.transaction_id} - {self.message_id}"


class OnConfirm(models.Model):
    transaction = models.ForeignKey(
        Transaction, on_delete=models.CASCADE, related_name="full_on_confirm"
    )
    message_id = models.CharField(max_length=100)
    payload = models.JSONField()
    timestamp = models.DateTimeField()

    def __str__(self):
        return f"{self.transaction.transaction_id} - {self.message_id}"


class OnStatus(models.Model):
    transaction = models.ForeignKey(
        Transaction, on_delete=models.CASCADE, related_name="full_on_status"
    )
    message_id = models.CharField(max_length=100)
    payload = models.JSONField()
    pan = models.CharField(max_length=20, blank=True, null=True)
    timestamp = models.DateTimeField()

    def __str__(self):
        return f"{self.transaction.transaction_id} - {self.message_id}"


class OnUpdate(models.Model):
    transaction = models.ForeignKey(
        Transaction, on_delete=models.CASCADE, related_name="full_on_update"
    )
    message_id = models.CharField(max_length=100)
    payload = models.JSONField()
    timestamp = models.DateTimeField()

    def __str__(self):
        return f"{self.transaction.transaction_id} - {self.message_id}"


class OnCancel(models.Model):
    transaction = models.ForeignKey(
        Transaction, on_delete=models.CASCADE, related_name="full_on_cancel"
    )
    message_id = models.CharField(max_length=100)
    payload = models.JSONField()
    timestamp = models.DateTimeField()

    def __str__(self):
        return f"{self.transaction.transaction_id} - {self.message_id}"


class PaymentSubmisssion(models.Model):
    transaction = models.ForeignKey(Transaction, on_delete=models.CASCADE)
    message_id = models.CharField(max_length=100)
    payment_id = models.CharField(max_length=100)
    status_pay = models.CharField(max_length=100)
    timestamp = models.DateTimeField()

    def __str__(self):
        return f"{self.transaction.transaction_id} - {self.submission_id}"
