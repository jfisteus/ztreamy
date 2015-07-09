var ztreamy = ztreamy || {}

// The Stream class
ztreamy.Stream = function(url) {
    this.url = url;
    this.maxDisplayedEvents = 200;
    this.lastSeen = null;
    this.numErrors = 0;
    this.atLeastOneSuccess = false;
    this.statusBar = null;
    this.status = "disconnected";
    this.aborted = false;
    this.running = false;

    this.consume = function() {
        if (this.running) {
            throw("Error: a request is already running");
        }
        if (this.aborted) {
            this.aborted = false;
        }
        var params = {};
        if (this.lastSeen) {
            params = {
                "last-seen": this.lastSeen,
                "past-events-limit": this.maxDisplayedEvents
            };
        }
        var stream = this;
        this.status = "connected";
        this.running = true;
        this.displayStatus();
        $.getJSON(this.url + '/long-polling', params)
            .done(function(events) {
                stream.running = false;
                if (!stream.aborted) {
                    stream.eventsCallback(events);
                    if (events.length > 0) {
                        stream.lastSeen =
                            events[events.length - 1]["Event-Id"];
                    }
                    stream.atLeastOneSuccess = true;
                    stream.numErrors = 0;
                    stream.consume();
                }
            })
            .fail(function(jqxhr, textStatus, error) {
                stream.running = false;
                stream.numErrors += 1;
                if ((stream.atLeastOneSuccess || textStatus === "timeout")
                    && stream.numErrors <= 3) {
                    // Try again
                    stream.consume();
                } else {
                    stream.status = textStatus + error;
                    stream.displayStatus();
                    stream.errorCallback(jqxhr, textStatus, error);
                }
            });
    }

    this.mostRecent = function(num, callback) {
        if (this.running) {
            throw("Error: a request is already running");
        }
        var num = parseInt(num);
        if (isNaN(num)) {
            throw "Not a number: num";
        }
        var stream = this;
        this.running = true;
        $.getJSON(this.url + '/long-polling', {
            "past-events-limit": num,
            "non-blocking": 1
        }).done(function(events) {
            stream.running = false;
            if (callback) {
                callback(events);
            } else {
                stream.eventsCallback(events);
            }
            if (events.length > 0) {
                stream.lastSeen = events[events.length - 1]["Event-Id"]
            }
            stream.atLeastOneSuccess = true;
        })
        .fail(function(jqxhr, textStatus, error) {
            stream.running = false;
            stream.numErrors += 1;
            stream.status = textStatus + error;
            stream.displayStatus();
            stream.errorCallback(jqxhr, textStatus, error);
        });
    }

    this.disconnect = function() {
        if (this.running) {
            this.aborted = true;
            this.status = "disconnected";
            this.displayStatus();
        }
    }

    this.connect = function () {
        if (!this.running) {
            this.consume();
        } else if (this.aborted) {
            this.aborted = false;
            this.status = "connected";
            this.displayStatus();
        }
    }

    this.displayStatus = function(statusBar) {
        if (statusBar) {
            this.statusBar = statusBar;
        } else if (!this.statusBar) {
            this.statusBar = $(".ztreamy-status");
        }
        if (this.statusBar.length > 0) {
            this.statusBar.find(".ztreamy-status-url").text(this.url);
            this.statusBar.find(".ztreamy-status-state").text(this.status);
            if (this.aborted | !this.running) {
                this.statusBar.find(".ztreamy-button-disconnect")
                    .prop("disabled", true);
                this.statusBar.find(".ztreamy-button-connect")
                    .prop("disabled", false);
            } else {
                this.statusBar.find(".ztreamy-button-disconnect")
                    .prop("disabled", false);
                this.statusBar.find(".ztreamy-button-connect")
                    .prop("disabled", true);
            }
        }
    }

    this.defaultEventsCallback = function(events) {
        if (events) {
            var insertionPoint = $(".ztreamy-events");
            if (insertionPoint.length > 0) {
                for (var i = 0; i < events.length; i++) {
                    insertionPoint.prepend(ztreamy.renderEvent(events[i]));
                }
                var all = insertionPoint.find(".ztreamy-event");
                if (all.length > this.maxDisplayedEvents) {
                    all.slice(this.maxDisplayedEvents - all.length).remove();
                }
            }
        }
    }

    this.eventsCallback = this.defaultEventsCallback;

    this.defaultErrorCallback = function(jqxhr, textStatus, error) {
        console.log("Error connecting to Ztreamy stream: " + textStatus +
                    " / " + error);
    }

    this.errorCallback = this.defaultErrorCallback;
}

ztreamy.renderEvent = function(event) {
    var card = $("<div>").addClass("ztreamy-event");
    // Render standard headers
    for (var i = 0; i < ztreamy.headers.length; i++) {
        if (ztreamy.headers[i] in event) {
            card.append(ztreamy.renderHeader(ztreamy.headers[i], event));
        }
    }
    // Render other headers
    for (var key in event) {
        if (ztreamy.headers.indexOf(key) === -1 && key !== "Body") {
            card.append(ztreamy.renderHeader(key, event));
        }
    }
    // Render body
    if ("Body" in event) {
        card.append(ztreamy.renderBody(event));
    }
    return card;
}

ztreamy.renderHeader = function(header, event) {
    var headerNode = $("<div>").addClass("ztreamy-event-header")
                               .text(event[header]);
    var headerNameNode = $("<span>").addClass("ztreamy-event-header-name")
                                    .text(header + ': ')
    headerNode.prepend(headerNameNode);
    return headerNode;
}

ztreamy.renderBody = function(event) {
    var body = event["Body"]
    if (event["Syntax"] === "application/json" ||
        event["Syntax"] === "application/ld+json") {
        body = JSON.stringify(body, null, 4);
    }
    return $("<div>").addClass("ztreamy-event-body")
                     .text(body);
}

ztreamy.headers = ["Event-Id",
                   "Source-Id",
                   "Application-Id",
                   "Timestamp",
                   "Syntax",
                   "Body-Length"
                   ]
