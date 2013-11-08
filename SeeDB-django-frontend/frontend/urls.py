from django.conf.urls import patterns, url

from frontend import views

urlpatterns = patterns('',
    url(r'^$', views.index, name='index'),
    url(r'^get_viz$', views.get_viz, name='get_viz'),
)
