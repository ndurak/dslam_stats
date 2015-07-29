from django.conf.urls import patterns, include, url

from melt import views

urlpatterns = patterns('',
    url(r'^$', views.index, name='index'),
)

#(?P<dslam>\S/+)/(?P<frame>\d)/(?P<slot>\d+)/(?P<port>\d+)/
