from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from py4j.java_gateway import JavaGateway

def index(request):
    # this should have a form to type a query
    return render(request, 'frontend/index.html', {})

@csrf_exempt
def get_viz(request):
    # send the query off to seedb and get the data
    if request.method == 'POST':
        query = request.POST['query']
        gateway = JavaGateway()
        processor = gateway.entry_point.GetQueryProcessor()
        processor.setQuery(query)
        result = processor.Process()
        print len(result)
        distributions = []
        # this is a list of DiscriminatingViews
        for view in result:
            # get the distribution
            view_ = {}
            view_["dist"] = getCombinedDistribution(view.getCombinedDistribution())
            view_["utility"] = view.getUtility()
            view_["group_by"] = view.getGroupByAttribute()
            view_["aggregate"] = view.getAggregateAttribute()
            distributions.append(view_)
        return render(request, 'frontend/results.html', {'views': distributions, 'query' : query})
    else:
        return HttpResponseRedirect('/frontend/')

def getCombinedDistribution(dist):
    ret = []
    for d in dist:
      s = d.split(":")
      ret = ret + [s]
    return ret
