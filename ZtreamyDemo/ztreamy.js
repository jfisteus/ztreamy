(function (exports, $) {

	var _streamUrl;
	var _eventHandler;
	var _initHandler;
	var _cancelHandler;
	var _timeout;

	var _requestUrl;
	var _configured = 0;
	var _started = 0;
	var _running = 0;

	exports.connect = function(streamUrl, eventHandler, initHandler, cancelHandler, timeout) {

		if ((streamUrl === undefined) || (streamUrl == null)) {
		    return;
		} else {
		    _streamUrl = streamUrl;
		    _requestUrl = streamUrl;
		}
	
		if ((eventHandler === undefined) || (eventHandler == null)) {
		    return;
		} else {
		    _eventHandler = eventHandler;
		}

		if ((timeout === undefined) || (timeout == null)) {
		    _timeout = 30000;
		} else {
		    _timeout = timeout;
		}
		
		_initHandler = initHandler;
		_cancelHandler = cancelHandler;
		_configured = 1;
	}


        exports.run = function poll() {
	   if (_configured == 1) {
	    	 $.ajax({ 
		    url: _requestUrl,
		    headers: { "Accept" : "application/json" },
	 	    beforeSend: function() {
			_running = 1;
			if (_started == 0) {
			   if ((_initHandler !== undefined) || (_initHandler != null)) {
			      _initHandler();
			   }
			}
		    },	
		    success: function(events){
			var _lastId;
			events.forEach(function(event) {
				_lastId = event["Event-Id"];
				_eventHandler(event);
			});

			if ((_lastId !== undefined) && (_lastId != null) && (_lastId != "")) {
				_requestUrl = _streamUrl + "?last-seen=" + _lastId;			
			}
			_started = 1;
    		    }, 
		    complete: function() {
			if (_running == 1) {
			   poll();
			} else {
			   if ((_cancelHandler !== undefined) || (_cancelHandler != null)) {
			      _cancelHandler();
			   }			   
			}
		    },
		    timeout: _timeout
        	 });
           }
	}

	exports.cancel = function() {
	   _running = 0;
	   _started = 0;
	   _requestUrl = _streamUrl;
	}

}(window.ztreamy = window.ztreamy || {}, jQuery));
