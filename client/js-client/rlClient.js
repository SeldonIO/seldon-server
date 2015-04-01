DEPENDENCIES = {};
TRACK_PAR = "rlabs";
QUERY_DELIMITER = "?";
TRACK_PAR_LIST = ["rlabs","zehtg"];

$.noConflict(function (r, p, e) {
    DEPENDENCIES = { require: r, provide: p, ender: e };
});

var rlClient = function () {
    var _ = DEPENDENCIES.ender._;
    var ajax = DEPENDENCIES.ender.ajax;

    // Configurable parameters (see init function below)
    var params = {
        // default endpoint
        endpoint: "http://localhost:8080/api-server",
        // consumer name
        consumer: "",
        // query keys to retain when constructing an item id
        retain: [],
        // url rewrite rules (list of objects)
        rewrite: [],
        // the delimiter for the rlabs tag i.e. hash in example.com#rlabs=1
        rlabs_delim: "?"
    };

    function extractQuery(paramString) {
        var params = {};
        _.each(paramString.split("&"), function (pair) {
            var x = pair.split("=");
            params[x[0]] = x[1];
        });
        return params;
    }

    function parameterString(params, keep) {
        var preserved = _.chain(_.keys(params).sort())
            .filter(function (x) { return _.contains(keep, x); })
            .map(function (key) { return key + "=" + params[key]; })
            .value();
        if ( preserved.length > 0 ) {
            return QUERY_DELIMITER + preserved.join("&");
        }
        return "";
    }

    function rewrite(url, rules) {
        var rewritten = url;
        for (var i = 0; i < rules.length; i++) {
            var rule = rules[i];
            for (var focus in rule) {
                var focusRx = new RegExp(focus);
                if (url.match(focusRx)) {
                    console.log(url.match(focusRx));
                    var target = rule[focus];
                    console.log("Rewriting: " + focus + " => " + target + " in " + url);
                    rewritten = rewritten.replace(focusRx, target);
                }
            }
        }
        return rewritten;
    }

    function normalise(input) {
        var delim = "#";
        if (QUERY_DELIMITER==="#"){
            delim = "?";
        }

        var normalised = input.replace(RegExp("\\" + delim + ".*"), "").replace(/[+ ]/, "%20", "g");
        return rewrite(normalised, params.rewrite);
    }

    function pageId(url, keep) {
        var normalised  = normalise(url);
        var source      = normalised.replace(RegExp("\\" + QUERY_DELIMITER + ".*"), "");
        var paramString = normalised.replace(RegExp(".*\\" + QUERY_DELIMITER), "");
        var params      = paramString ? extractQuery(paramString) : {};
        var outParams   = parameterString(params, keep);
        var pageId      = source + outParams;

        return encodeURIComponent(pageId);
    }

    function currentPageId(keep) {
        return pageId(window.location.href, keep);
    }

    function fullEndpoint(path) {
        return params.endpoint + path + "?consumer_key=" + params.consumer;
    }

    function actionUrl(type, user_id, item_id, rlabs, source) {
        return fullEndpoint("/js/action/new") + 
            "&type=" + type +
            "&user=" + user_id +
            "&item=" + item_id +
            (rlabs ? ("&"+TRACK_PAR+"=" + rlabs) : "") +
            (source ? ("&source=" + encodeURIComponent(normalise(source))) : "") ;;
    }

    function userUrl(user_id, facebook_opts) {
        return fullEndpoint("/js/user/new") +
            "&id="      + user_id +
            "&fb="      + (facebook_opts["facebook"] || "") +
            "&fbId="    + (facebook_opts["facebook_id"] || "") +
            "&fbToken=" + (facebook_opts["facebook_token"] || "");
    }

    function itemUrl(item_id, title, type) {
        return fullEndpoint("/js/item/new") +
            "&id=" + item_id +
            "&title=" + (title || "") +
            "&type=" + type;
    }

    function recommendedUsersUrl(user_id, item_id) {
        return fullEndpoint("/js/share") +
            "&user=" + user_id +
            "&item=" + item_id;
    }

    function attributeString(attributeNames) {
        return attributeNames.join(",");
    }

    function recommendationsUrl(user_id, item_id, rlabs, options) {
        return fullEndpoint("/js/recommendations") +
            "&user=" + user_id +
            "&item=" + item_id +
            "&dimension=" + (options["dimension"] || 0) +
            "&limit=" + (options["limit"] || 10) +
            (options["attributes"] ? ("&attributes=" + attributeString(options["attributes"])) : "") +
            (options["algorithms"] ? "&algorithms=" + options["algorithms"] : "") +
            (rlabs ? ("&"+TRACK_PAR+"=" + rlabs) : "");
    }

    function jsonpCall(url, callback) {
        var stamp = new Date().getTime();
        var stamped = url + (url.match(/\?/) ? "&" : "?") + "timestamp=" + stamp;
        
        ajax({
            url: stamped,
            type: 'jsonp',
            jsonpCallback: 'jsonpCallback',
            success: callback
        });
    }

    function withMandatory(name, value, callback) {
        if (!value) {
            return { fired: false, message: "No " + name + " specified." };
        }
        callback();
        return { fired: true };
    }

    function addAction(type, callback, user_id, item_id, rlabs, source) {
        return withMandatory("user_id", user_id, function() {
            var id = item_id || currentPageId(params.retain);
            var recId = rlabs;
            if(recId == undefined || recId) {
                recId = rlabsId();
            }
            var url = actionUrl(type, user_id, id, recId, source);
            jsonpCall(url, callback);
        });
    }

    function recommendedUsers(user_id, item_id, callback) {
        return withMandatory("user_id", user_id, function() {
            var id = item_id || currentPageId(params.retain);
            var url = recommendedUsersUrl(user_id, id);
            jsonpCall(url, callback);
        });
    }

    function addUser(user_id, options, callback) {
        return withMandatory("user_id", user_id, function() {
            var url = userUrl(user_id, options || {});
            jsonpCall(url, callback);
        });
    }

    function addItem(item_id, type, title, callback) {
        return withMandatory("item_id", item_id, function() {
            var url = itemUrl(item_id, title, type);
            jsonpCall(url, callback);
        });
    }

    function rlabsId() {
        var normalised  = document.location.href;
        var query = normalised.replace(RegExp(".*\\" + params.rlabs_delim), "");
        var rlabs = undefined;
        if(params.rlabs_delim == "?") {
            // removing final # if present in the query parameter
            query = query.replace(RegExp("#" + ".*"), "");
        }
        if ( query !== "" ) {
            var query_params = extractQuery(query);
            for (var i = 0; i < TRACK_PAR_LIST.length; i++) {
                var val = query_params[TRACK_PAR_LIST[i]];
                if(val) { rlabs = val; }
            }
        }
        return rlabs;
    }

    function recommendationsFor(user_id, callback, options) {
        return withMandatory("user_id", user_id, function() {
            var itemId = options["item"] || currentPageId(params.retain);
            var rlabs = options[TRACK_PAR] || rlabsId();
            var url = recommendationsUrl(user_id, itemId, rlabs, options);
            jsonpCall(url, callback);
        });
    }

    function recommendationsStatic(callback,options,dimension) {
        var o = options || {};
        var url = "https://s3-eu-west-1.amazonaws.com/rl-static/" + params.consumer + ".json?jsonpCallback\=unused";
        if(dimension) {
            url = "https://s3-eu-west-1.amazonaws.com/rl-static/" + params.consumer + "-"+dimension+".json?jsonpCallback\=unused";
        }
        ajax({
            url: url,
            type: 'jsonp',
            jsonpCallback: 'jsonpCallback',
            success: function(response) {
                var recommendations = [];
                if (response["error_id"] === undefined) {
                    recommendations = response["list"];
                    if ( o["appendClick"] ) {
                        appendClickTo(recommendations,o);
                    }
                } else {
                    if ( 'console' in window ) {
                        console.log("Problem retrieving recommendations: " + response["message"]);
                    }
                }
                callback(recommendations);
            }
        });
    }

    function appendClickTo(items,options) {
        var delim = params.rlabs_delim;
        for (var i = 0; i < items.length; i++) {
            var item = items[i];
            var id  = item["id"];
            var uuid = item["attributesName"]["recommendationUuid"];
            if ( uuid ) {
                id += id.match(RegExp("\\" + QUERY_DELIMITER)) ? "&" : delim;
                id += TRACK_PAR+"=" + uuid + "";
                
            }
            item["id"] = id;
        }
    }

    function recommendations(user_id, callback, options) {
        var o = options || {};
        return recommendationsFor(user_id, function (response) {
            var recommendations = [];
            if ((response!=null) && (response!=undefined) && (response["error_id"] === undefined)) {
                recommendations = response["list"];
                if ( o["appendClick"] ) {
                    appendClickTo(recommendations, o);
                }
            } else {
                if ( ('console' in window) && (response!=null) && (response!=undefined) ) {
                    console.log("Problem retrieving recommendations: " + response["message"]);
                }
            }
            callback(recommendations);
        }, o);
    }

    return {
        init: function (overrides) {
            _.each(overrides, function (val, key) { params[key] = val; });
        },
        pageId: pageId,
        endpoint: function() { return params["endpoint"]; },
        addUser: addUser,
        addItem: addItem,
        recommendedUsers: recommendedUsers,
        addAction: addAction,
        //recommendationsFor: recommendationsFor
        recommendations: recommendations,
        recommendationsStatic: recommendationsStatic
    };
}();
