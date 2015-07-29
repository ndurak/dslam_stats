from django.conf.urls import patterns, include, url

# Uncomment the next two lines to enable the admin:
from django.contrib import admin
admin.autodiscover()

urlpatterns = patterns('',
    # Examples:
    # url(r'^$', 'config.views.home', name='home'),
    url(r'^', include('dslam_stats.urls')),
    url(r'^lols/', include('lols.urls')),
    url(r'^dostupnost/', include('dostupnost.urls')),
    url(r'^melt/', include('melt.urls')),
    # url(r'^config/', include('config.foo.urls')),

    # Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    url(r'^admin/', include(admin.site.urls)),
)
