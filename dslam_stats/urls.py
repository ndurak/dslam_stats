from django.conf.urls import patterns, include, url

from dslam_stats import views

urlpatterns = patterns('',
    url(r'^$', views.index, name='index'),
    url(r'^detail', views.detail, name='detail')
)

#(?P<dslam>\S/+)/(?P<frame>\d)/(?P<slot>\d+)/(?P<port>\d+)/
