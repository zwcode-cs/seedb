$('.header-inner').append('<button type="button" class="js-recommended-item" onclick="toggleShow();">Toggle</button>')

var first = true;

var groups = ['VentureBeat', 'harpersbazaarus', 'BostInno', 'Inc', 'Forbes', 'BW', 'TIME', 'CNET', 'BBCWorld', 'seattletimes', 'BBCNewsUS', 'themotleyfool', 'WhoWhatWear', 'WIRED', 'YahooFinance', 'PopSci', 'TechCrunch'];


function toggleShow() {
	$('div.stream > ol > li.stream-item').each(function(index) {
		var name = $(this).find('div').data('screenName');

		if (name != '') {
			if (first) {
				if ($.inArray(name, groups) > -1) $(this).toggle();
			}
			else {
				$(this).toggle();
			}
		}
	});
	first = false;
};

	if ($.inArray(name, groups) > -1) $(this).toggle();
	else $(this).show();

	$(this).html(name)
	$(this).remove();
$.each(tweets, function(idx, tweet) {return $(tweet).find('div')})