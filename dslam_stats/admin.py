from django.contrib import admin
from dslam_stats.models import Dslami, DslamType, Port
from dslam_stats.models import DataRateProfile, LineSpectrumProfile, SNRMarginProfile, InpProfile, XdslRtx

class DslamiAdmin(admin.ModelAdmin):
    list_display = ['dslam_id', 'ime', 'ip']

admin.site.register(Dslami, DslamiAdmin)

class DslamTypeAdmin(admin.ModelAdmin):
    list_display = ['name']

admin.site.register(DslamType, DslamTypeAdmin)
admin.site.register(Port)



class DataRateProfileAdmin(admin.ModelAdmin):
    list_display = ['name']
    fields = ['name', 'max_data_rate', 'min_data_rate']


admin.site.register(DataRateProfile, DataRateProfileAdmin)

class LineSpectrumProfileAdmin(admin.ModelAdmin):
    list_display = ['id', 'name']
    fields = ['id', 'name']

admin.site.register(LineSpectrumProfile, LineSpectrumProfileAdmin)

class SNRMarginProfileAdmin(admin.ModelAdmin):
    list_display = ['id', 'name']
    fields = ['id', 'name', 'target_snr_ds', 'target_snr_us', 'max_snr_ds', 'max_snr_us',
              'min_snr_ds', 'min_snr_us', 'rate_upshift_snr', 'rate_downshift_snr',
              'ra_mode_ds', 'ra_mode_us']

admin.site.register(SNRMarginProfile, SNRMarginProfileAdmin)

class InpProfileAdmin(admin.ModelAdmin):
    list_display = ['id', 'name']
    fields = ['id', 'name', 'min_inp_shine_ds', 'min_inp_shine_us', 'max_delay_ds', 'max_delay_us']

admin.site.register(InpProfile,InpProfileAdmin)

class XdslRtxAdmin(admin.ModelAdmin):
    list_display = ['id', 'name']
    fields = ['id', 'name']

admin.site.register(XdslRtx, XdslRtxAdmin)

