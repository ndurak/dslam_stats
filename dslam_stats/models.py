from django.db import models
import custom


class DslamType(models.Model):
    id = custom.PositiveTinyIntegerField(primary_key=True)
    name = models.CharField(max_length=20L, blank=True)
    class Meta:
        db_table = 'dslam_type'
    def __unicode__(self):
        return self.name

class Dslami(models.Model):
    dslam_id = models.SmallIntegerField(primary_key=True)
    name = models.CharField(max_length=100L, unique=True, blank=True)
    dslam_type = models.ForeignKey(DslamType)
    ip = models.CharField(max_length=20L, blank=True)
    videoip = models.CharField(max_length=15L, blank=True)
    manag_ip = models.CharField(max_length=20L, blank=True)
    voice_ip = models.CharField(max_length=20L, blank=True)
    video_ip = models.CharField(max_length=20L, blank=True)
    video_ip2 = models.CharField(max_length=20L, blank=True)
    region = models.CharField(max_length=10L, blank=True)
    sub_device = models.CharField(max_length=50L, blank=True)
    class Meta:
        db_table = 'dslami'
    def __unicode__(self):
        return self.ime


class DslamBoards(models.Model):
    dslam_id = models.ForeignKey(Dslami)
    board = models.CharField(max_length=5L)
    type = models.CharField(max_length=10L, blank=True)
    class Meta:
        db_table = 'dslam_boards'
        unique_together=(("dslam_id", "board"),)
    def __unicode__(self):
        return self.name


class Port(models.Model):
    id = models.SmallIntegerField(primary_key=True)
    ifIndex = models.PositiveIntegerField(db_column='ifIndex') # Field name made lowercase.
    name = models.CharField(max_length=7L, blank=True)
    type = models.SmallIntegerField()
    class Meta:
        db_table = 'ports'
    def __unicode__(self):
        if self.ifIndex >= 201326592 and self.ifIndex <= 201478080:
            return self.name +u' adsl'
        else:
            return self.name +u' vdsl'

class DataRateProfile(models.Model):
    id = models.SmallIntegerField(primary_key=True)
    name = models.CharField(max_length=20)
    max_data_rate = models.IntegerField()
    min_data_rate = models.IntegerField()
    class Meta:
        db_table = 'vop_rate'
    def __unicode__(self):
        return self.name

class LineSpectrumProfile(models.Model):
    id = models.SmallIntegerField(primary_key=True)
    name = models.CharField(max_length=20)
    class Meta:
        db_table = 'vop_spectrum'
    def __unicode__(self):
        return self.name

class SNRMarginProfile(models.Model):
    id = models.SmallIntegerField(primary_key=True)
    name = models.CharField(max_length=20)
    target_snr_ds = models.DecimalField(max_digits=5, decimal_places=1)
    target_snr_us = models.DecimalField(max_digits=5, decimal_places=1)
    max_snr_ds = models.DecimalField(max_digits=5, decimal_places=1)
    max_snr_us = models.DecimalField(max_digits=5, decimal_places=1)
    min_snr_ds = models.DecimalField(max_digits=5, decimal_places=1, blank=True)
    min_snr_us = models.DecimalField(max_digits=5, decimal_places=1, blank=True)
    rate_upshift_snr = models.DecimalField(max_digits=5, decimal_places=1, blank=True)
    rate_downshift_snr = models.DecimalField(max_digits=5, decimal_places=1, blank=True)
    ra_mode_ds = custom.TinyIntegerField()
    ra_mode_us = custom.TinyIntegerField()
    class Meta:
        db_table = 'vop_snr'
    def __unicode__(self):
        return self.name

class InpProfile(models.Model):
    id = models.SmallIntegerField(primary_key=True)
    name = models.CharField(max_length=20)
    min_inp_shine_ds = custom.PositiveTinyIntegerField()
    min_inp_shine_us = custom.PositiveTinyIntegerField()
    max_delay_ds = custom.PositiveTinyIntegerField()
    max_delay_us = custom.PositiveTinyIntegerField()
    class Meta:
        db_table = 'vop_inp'
    def __unicode__(self):
        return self.name

# Create your models here.
class VdslStats(models.Model):
    time = models.DateTimeField(auto_now_add=True)
    dslam_id = models.SmallIntegerField()
    port_id = models.PositiveIntegerField()
    data_rate_profile_ds = models.ForeignKey(DataRateProfile, related_name='vrate_profile_ds')
    data_rate_profile_us = models.ForeignKey(DataRateProfile, related_name='vrate_profile_us')
    noise_margin_profile = models.ForeignKey(SNRMarginProfile)
    inp_profile = models.ForeignKey(InpProfile)
    line_spectrum_profile = models.ForeignKey(LineSpectrumProfile)
    status = custom.PositiveTinyIntegerField(default=4)
    ln_atten_ds = models.DecimalField(max_digits=3, decimal_places=1, default=0.0)
    ln_atten_us = models.DecimalField(max_digits=3, decimal_places=1, default=0.0)
    kl0_co = models.DecimalField(max_digits=3, decimal_places=1, default=0.0)
    kl0_cpe = models.DecimalField(max_digits=3, decimal_places=1, default=0.0)
    snr_margin_ds = models.DecimalField(max_digits=3, decimal_places=1, default=0.0)
    snr_margin_us = models.DecimalField(max_digits=3, decimal_places=1, default=0.0)
    act_atp_ds = models.DecimalField(max_digits=5, decimal_places=1, default=0.0)
    act_atp_us = models.DecimalField(max_digits=5, decimal_places=1, default=0.0)
    rtx_used_ds = custom.TinyIntegerField()
    rtx_used_us = custom.TinyIntegerField()
    ra_mode_ds = custom.TinyIntegerField()
    ra_mode_us = custom.TinyIntegerField()
    attainable_rate_ds = models.PositiveIntegerField(default=0)
    attainable_rate_us = models.PositiveIntegerField(default=0)
    act_data_rate_ds = models.PositiveIntegerField(default=0)
    act_data_rate_us = models.PositiveIntegerField(default=0)
    act_delay_ds = custom.PositiveTinyIntegerField(default=0)
    act_delay_us = custom.PositiveTinyIntegerField(default=0)
    inp_ds = models.DecimalField(max_digits=4, decimal_places=1, default=0.0)
    inp_us = models.DecimalField(max_digits=4, decimal_places=1, default=0.0)
    inp_rein_ds = models.DecimalField(max_digits=3, decimal_places=1, default=0.0)
    inp_rein_us = models.DecimalField(max_digits=3, decimal_places=1, default=0.0)
    es_ds = models.IntegerField()
    es_us = models.IntegerField()
    ses_ds = models.IntegerField()
    ses_us = models.IntegerField()
    init_times = models.IntegerField()
    class Meta:
        db_table=u'vdsl_stats'


class AdslStats(models.Model):
    time = models.DateTimeField(auto_now_add=True)
    dslam_id = models.SmallIntegerField()
    port_id = models.PositiveIntegerField()
    data_rate_profile_ds = models.ForeignKey(DataRateProfile, related_name='arate_profile_ds')
    data_rate_profile_us = models.ForeignKey(DataRateProfile, related_name='arate_profile_us')
    noise_margin_profile = models.ForeignKey(SNRMarginProfile)
    inp_profile = models.ForeignKey(InpProfile)
    line_spectrum_profile = models.ForeignKey(LineSpectrumProfile)
    status = custom.PositiveTinyIntegerField(default=4)
    ln_atten_ds = models.DecimalField(max_digits=3, decimal_places=1, default=0.0)
    ln_atten_us = models.DecimalField(max_digits=3, decimal_places=1, default=0.0)
#    kl0_co = models.DecimalField(max_digits=3, decimal_places=1, default=0.0)
#    kl0_cpe = models.DecimalField(max_digits=3, decimal_places=1, default=0.0)
    snr_margin_ds = models.DecimalField(max_digits=3, decimal_places=1, default=0.0)
    snr_margin_us = models.DecimalField(max_digits=3, decimal_places=1, default=0.0)
    act_atp_ds = models.DecimalField(max_digits=5, decimal_places=1, default=0.0)
    act_atp_us = models.DecimalField(max_digits=5, decimal_places=1, default=0.0)
    rtx_used_ds = custom.TinyIntegerField()
#    rtx_used_us = custom.TinyIntegerField()
#    ra_mode_ds = custom.TinyIntegerField()
#    ra_mode_us = custom.TinyIntegerField()
    attainable_rate_ds = models.PositiveIntegerField(default=0)
    attainable_rate_us = models.PositiveIntegerField(default=0)
    act_data_rate_ds = models.PositiveIntegerField(default=0)
    act_data_rate_us = models.PositiveIntegerField(default=0)
    act_delay_ds = custom.PositiveTinyIntegerField(default=0)
    act_delay_us = custom.PositiveTinyIntegerField(default=0)
    inp_ds = models.DecimalField(max_digits=4, decimal_places=1, default=0.0)
    inp_us = models.DecimalField(max_digits=4, decimal_places=1, default=0.0)
    inp_rein_ds = models.DecimalField(max_digits=3, decimal_places=1, default=0.0)
    inp_rein_us = models.DecimalField(max_digits=3, decimal_places=1, default=0.0)
    es_ds = models.IntegerField()
    es_us = models.IntegerField()
    ses_ds = models.IntegerField()
    ses_us = models.IntegerField()
    init_times = models.IntegerField()
    class Meta:
        db_table=u'adsl_stats'


class XdslRtx(models.Model):
    id = models.IntegerField(primary_key=True)
    name = models.CharField(max_length=30L, blank=True)
    class Meta:
        db_table = 'xdsl_rtx'

