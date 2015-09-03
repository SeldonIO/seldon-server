/*global reqwest:false, underscore:false, window:false, document:false */
var rlClient = (function () {
    "use strict";
    var ajax = reqwest,

    // Configurable parameters (see init function below)
        params = {
            // default endpoint
            endpoint: "http://localhost:8080/api-server",
            // consumer name
            consumer: "",
            // query keys to retain when constructing an item id
            retain: [],
            // url rewrite rules (list of objects)
            rewrite: [],
            // the delimiter for the rlabs tag i.e. hash in example.com#rlabs=1
            rlabs_delim: "?",
            // the tag to look for when deciding whether this page view has come from
            // a rec click
            track_par: "rlabs",
            // possible track pars (to remove??)
            track_par_list: ["rlabs", "zehtg"],
            //  not sure
            query_delimiter: "?",
            rectag_name: "rectag",
            use_pos_name: "use_pos",
            current_page_url: location.href
        };

    function extractQuery(paramString) {
        var q_params = {};
        underscore.each(paramString.split("&"), function (pair) {
            var x = pair.split("=");
            q_params[x[0]] = x[1];
        });
        return q_params;
    }

    function retrieveSeldonParamsFromURL() {
        var normalised = params.current_page_url,
            rlabs,
            query_params,
            query = normalised.replace(new RegExp(".*\\" + params.rlabs_delim), "");
        if (params.rlabs_delim === "?") {
            // removing final # if present in the query parameter
            query = query.replace(new RegExp("#" + ".*"), "");
        }
        if (query !== "") {
            query_params = extractQuery(query);
            underscore.each(params.track_par_list, function (par) {
                var val = query_params[par];
                if (val) {
                    rlabs = val;
                }
            });
        }
        return rlabs ? rlabs.split("%20") : [];
    }

    function parameterString(params, keep) {
        var preserved = underscore.chain(underscore.keys(params).sort())
            .filter(function (x) {
                return underscore.contains(keep, x);
            })
            .map(function (key) {
                return key + "=" + params[key];
            })
            .value();
        if (preserved.length > 0) {
            return params.query_delimiter + preserved.join("&");
        }
        return "";
    }

    function rewrite(url, rules) {
        var rewritten = url;
        underscore.each(rules, function (rule) {
            underscore.each(rule, function (focus) {
                var focusRx = new RegExp(focus);
                if (url.match(focusRx)) {
                    rewritten = rewritten.replace(focusRx, rule[focus]);
                }
            });
        });

        return rewritten;
    }

    function normalise(input) {
        var normalised, delim = "#";
        if (params.query_delimiter === "#") {
            delim = "?";
        }

        normalised = input.replace(new RegExp("\\" + delim + ".*"), "").replace(/[+ ]/, "%20", "g");
        return rewrite(normalised, params.rewrite);
    }

    function pageId(url, keep) {
        var normalised = normalise(url),
            source = normalised.replace(new RegExp("\\" + params.query_delimiter + ".*"), ""),
            paramString = normalised.replace(new RegExp(".*\\" + params.query_delimiter), ""),
            q_params = paramString ? extractQuery(paramString) : {},
            outParams = parameterString(q_params, keep),
            page_id = source + outParams;

        return encodeURIComponent(page_id);
    }

    function currentPageId(keep) {
        return pageId(params.current_page_url, keep);
    }

    function fullEndpoint(path) {
        return params.endpoint + path + "?consumer_key=" + params.consumer;
    }

    function actionUrl(type, user_id, item_id, rlabs, source, rectag, position) {
        return fullEndpoint("/js/action/new") +
            "&type=" + type +
            "&user=" + user_id +
            "&item=" + item_id +
            (rlabs ? ("&" + params.track_par + "=" + rlabs) : "") +
            (rectag ? ("&rectag=" + rectag) : "") +
            (source ? ("&source=" + encodeURIComponent(normalise(source))) : "") +
            (position ? ("&pos=" + position) : "");
    }

    function userUrl(user_id, facebook_opts) {
        return fullEndpoint("/js/user/new") +
            "&id=" + user_id +
            "&fb=" + (facebook_opts.facebook || "") +
            "&fbId=" + (facebook_opts.facebook_id || "") +
            "&fbToken=" + (facebook_opts.facebook_token || "");
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

    function recommendationsUrl(user_id, item_id, rlabs, rectag, options) {
        return fullEndpoint("/js/recommendations") +
            "&user=" + user_id +
            "&item=" + item_id +
            "&dimensions=" + ((options.dimensions) || (options.dimension || 0)) +
            "&limit=" + (options.limit || 10) +
            (options.attributes ? ("&attributes=" + attributeString(options.attributes)) : "") +
            (options.algorithms ? "&algorithms=" + options.algorithms : "") +
            (rectag ? ("&rectag=" + rectag) : "") +
            (rlabs ? ("&" + params.track_par + "=" + rlabs) : "") +
            (options.cohort ? ("&cohort=" + options.cohort) : "");
    }

    function jsonpCall(url, callback) {
        var stamp = new Date().getTime(),
            stamped = url + (url.match(/\?/) ? "&" : "?") + "timestamp=" + stamp;

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

    function addAction(type, callback, user_id, item_id, rlabs, source, rectag, pos) {
        return withMandatory("user_id", user_id, function () {
            var urlparams = retrieveSeldonParamsFromURL(),
                id = item_id || currentPageId(params.retain),
                recId = rlabs || urlparams[0],
                tag = rectag || urlparams[1],
                position = pos || urlparams[2],
                url;

            url = actionUrl(type, user_id, id, recId, source, tag, position);
            jsonpCall(url, callback);
        });
    }

    function recommendedUsers(user_id, item_id, callback) {
        return withMandatory("user_id", user_id, function () {
            var id = item_id || currentPageId(params.retain),
                url = recommendedUsersUrl(user_id, id);
            jsonpCall(url, callback);
        });
    }

    function addUser(user_id, options, callback) {
        return withMandatory("user_id", user_id, function () {
            var url = userUrl(user_id, options || {});
            jsonpCall(url, callback);
        });
    }

    function addItem(item_id, type, title, callback) {
        return withMandatory("item_id", item_id, function () {
            var url = itemUrl(item_id, title, type);
            jsonpCall(url, callback);
        });
    }



    function recommendationsFor(user_id, callback, options) {
        return withMandatory("user_id", user_id, function () {
            var itemId = options.item || currentPageId(params.retain),
                paramsFromURL = retrieveSeldonParamsFromURL(),
                rlabs = options[params.track_par] || paramsFromURL[0],
                rectag = options[params.rectag_name] || paramsFromURL[1],
                url = recommendationsUrl(user_id, itemId, rlabs, rectag, options);
            jsonpCall(url, callback);
        });
    }

    function appendClickTo(items, options) {
        var delim = params.rlabs_delim,
            rectag = options[params.rectag_name],
            use_pos = options[params.use_pos_name],
            pos = 0;
        underscore.each(items, function (item) {
            var id = item.id,
                uuid = item.attributesName.recommendationUuid;
            if (uuid) {
                id += id.match(new RegExp("\\" + params.query_delimiter)) ? "&" : delim;
                id += params.track_par + "=" + uuid;
                if (rectag) {
                    id += "%20" + rectag;
                }
                if (use_pos) {
                    pos += 1;
                    id += "%20" + pos;
                }
            }
            item.id = id;
        });
    }

    function recommendationsStatic(callback, options, dimension) {
        var o = options || {},
            url = "https://s3-eu-west-1.amazonaws.com/rl-static/" + params.consumer + ".json?jsonpCallback=unused";
        if (dimension) {
            url = "https://s3-eu-west-1.amazonaws.com/rl-static/" + params.consumer + "-" + dimension + ".json?jsonpCallback=unused";
        }
        ajax({
            url: url,
            type: 'jsonp',
            jsonpCallback: 'jsonpCallback',
            success: function (response) {
                var recommendations = [];
                if (response.error_id === undefined) {
                    recommendations = response.list;
                    if (o.appendClick) {
                        appendClickTo(recommendations, options);
                    }
                }
                callback(recommendations);
            }
        });
    }

    function recommendationsStaticCustomUrl(custom_url, callback, options) {
        var o = options || {},
            stamp = new Date().getTime();
        custom_url = custom_url + (custom_url.match(/\?/) ? "&" : "?") + "timestamp=" + stamp + "&jsonpCallback=unused";
        ajax({
            url: custom_url,
            type: 'jsonp',
            jsonpCallback: 'jsonpCallback',
            success: function (response) {
                var recommendations_list = [],
                    cohort = "-";
                if ((response !== null) && (response !== undefined) && (response.error_id === undefined)) {
                    if (response.list) {
                        recommendations_list = response.list;
                    } else {
                        recommendations_list = response.recommendedItems;
                        cohort = response.cohort;
                    }

                    if (o.appendClick) {
                        appendClickTo(recommendations_list, options);
                    }
                }
                callback({recs: recommendations_list, cohort: cohort, service_type: "fallback"});
            }
        });
    }

    function recommendations(user_id, callback, options) {
        var o = options || {};
        return recommendationsFor(user_id, function (response) {
            var recommendations_list = [],
                cohort = "-";
            if ((response !== null) && (response !== undefined) && (response.error_id === undefined)) {
                if (response.list) {
                    recommendations_list = response.list;
                } else {
                    recommendations_list = response.recommendedItems;
                    cohort = response.cohort;
                }

                if (o.appendClick) {
                    appendClickTo(recommendations_list, options);
                }
            }
            callback({recs: recommendations_list, cohort: cohort});
        }, o);
    }

    return {
        init: function (overrides) {
            underscore.each(overrides, function (val, key) {
                params[key] = val;
            });
        },
        pageId: pageId,
        endpoint: function () {
            return params.endpoint;
        },
        addUser: addUser,
        addItem: addItem,
        recommendedUsers: recommendedUsers,
        addAction: addAction,
        recommendations: recommendations,
        recommendationsStatic: recommendationsStatic,
        recommendationsStaticCustomUrl: recommendationsStaticCustomUrl
    };
}());
