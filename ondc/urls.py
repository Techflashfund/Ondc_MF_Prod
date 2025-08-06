from django.urls import path

from .views import *

# from igmflow.views import OnIssueStatusView

urlpatterns = [
    path("search/", ONDCSearchView.as_view()),
    path("on_search", OnSearchView.as_view(), name="on_search"),
    # SIP Creation
    path("on_searchdata", OnSearchDataView.as_view(), name="on_search_data"),
    path("select/", SIPCreationView.as_view(), name="select"),
    path("on_select", OnSelectSIPView.as_view(), name="on_select"),
    path("formsub", FormSubmisssion.as_view(), name="formsub"),
    path("init/", INIT.as_view(), name="init"),
    path("on_init", ONINIT.as_view(), name="on_init"),
    path("confirm", ConfirmSIP.as_view(), name="confirm"),
    path("on_confirm", OnConfirmSIP.as_view(), name="on_confirm"),
    path("on_status", OnStatusView.as_view(), name="on_status"),
    path("on_update", OnUpdateView.as_view(), name="on_update"),
    path("digisend", DigiLockerFormSubmission.as_view(), name="digisend"),
    path("esignsub", EsignFormSubmission.as_view(), name="esignsub"),
    # Lumpsum
    path("lumpselect", Lumpsum.as_view(), name="lumpselect"),
    path("lumpformsub", LumpFormSub.as_view(), name="lumpformsub"),
    path("lumpinit", LumpINIT.as_view(), name="lumpinit"),
    path("lumpconfirm", ConfirmLump.as_view(), name="lumpconfirm"),
    # Sip with Existing Folio
    path("sipexistinit", SIPExixstingInit.as_view(), name="sipexistinit"),
    path("sipexistconfirm", SIPExistingConfirm.as_view(), name="sipexistconfirm"),
    # SIP Cancel By Investor
    path("sipcancel", SIPCancel.as_view(), name="sipcancel"),
    path("on_cancel", OnCancelView.as_view(), name="on_cancel"),
    # Lumpsum with KYC
    path("lumpdigisend", LumpsumDigiLockerSubmission.as_view(), name="lumpdigisub"),
    path("lumpesignsub", LumpsumEsignFormSubmission.as_view(), name="lempesignsub"),
    # # Lumpsum with Existing Folio
    path("lumpexistinit", LumpsumExistingFolioInit.as_view(), name="lumpexistinit"),
    path("lumpconfirmexist", LumpConfirmExisting.as_view(), name="lumpconfirmexist"),
    # # Redemption
    path("redemselect", RedemptionSelect.as_view(), name="redemselect"),
    path("redempinit", RedemptionInit.as_view(), name="redempinit"),
    path("redempconfirm", RedemptionConfirm.as_view(), name="redempconfirm"),
    # LumpRetry
    path("lumpretryinit", LumpRetryInit.as_view(), name="lumpretryinit"),
    path("lumpretryconfirm", LumpRetryConfirm.as_view(), name="lumpretryconfirm"),
    path("lumpupdate", LumpRetryUpdate.as_view(), name="lumpupdate"),
    path("completesip", CompleteSIPFlowView.as_view(), name="complete"),
    # igm
    # path('on_issue_status',OnIssueStatusView.as_view(),name='on_issue_status'),
    # ON methods dataviews (on_select,on_init,on_confirm,on_status,on_update,on_cancel)
    path("onselect", OnSelectDataView.as_view(), name="on_select_data"),
    path("oninit", OnInitDataView.as_view(), name="on_init_data"),
    path("onconfirm", OnConfirmDataView.as_view(), name="on_confirm_data"),
    path("onstatus", OnStatusDataView.as_view(), name="on_status_data"),
    path("onupdate", OnUpdateDataView.as_view(), name="on_update_data"),
    path("oncancel", OnCancelDataView.as_view(), name="oncancel"),
    # satus api added
    path("status", StatusAPIView.as_view(), name="status"),
]
