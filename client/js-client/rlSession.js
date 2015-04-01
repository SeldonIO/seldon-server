var windowProxy; // TODO rename
var rlSession = (function () {
    var rlId          = "rlId";
    var rlId_loggedIn = "rlId_loggedin";

    // Extracted from jquery-cookie, https://github.com/carhartl/jquery-cookie
    function readCookie(name) {
        var cookies = document.cookie.split('; ');
        for (var i = 0, parts; (parts = cookies[i] && cookies[i].split('=')); i++) {
            if (parts.shift() === name) {
                return parts.join('=');
            }
        }
        return undefined;
    }

    function hostDomain() {
    	if(window.location.hostname == "localhost") { 
    		return window.location.hostname; 
    	}
        var capture = window.location.hostname.match("^[^\.]+(\.[^\.]+\.[^\.]+)$");
        return capture && capture [1];
    }

    function setCookie(cookieName, userId, months, days) {
        var now = new Date();
        months = months || 12;
        now.setMonth(now.getMonth() + months);
        days = days || 0;
        now.setDate(now.getDate()+ days);
        var domainPair = "";
        var domain = hostDomain();
        if ( domain ) {
            domainPair = "; domain=" + domain;
        }
        document.cookie = cookieName + "=" + userId + domainPair + "; expires=" + now.toUTCString() + "; path=/";
        //check that the cookie was actually written
        // see http://stackoverflow.com/questions/8944974/using-appspot-com-as-a-partial-domain-cookie
        if(!readCookie(cookieName)){
            document.cookie = cookieName + "=" + userId + "; expires=" + now.toUTCString() + "; path=/";
        }
    }

    function deleteCookie(cookieName) {
        var now = new Date().toUTCString();
        var domainPair = "";
        var domain = hostDomain();
        if ( domain ) {
            domainPair = "; domain=" + domain;
        }
        document.cookie = cookieName + "=" + 0 + domainPair + "; expires=" + now + "; path=/";
    }

    function replicateCookie(userId, months) {
        return setCookie(rlId, userId, months);
    }

    /**
     * Very naive at the moment:
     * (a) is the vendor Apple (is this Safari?);
     * (b) if so, assume a block if the rlOk cookie is absent.
     */
    function privacyBlock() {
        var vendor = navigator["vendor"];
        return ( vendor !== undefined && vendor.match(/^Apple/) && !readCookie("rlOk") );
    }

    function requestedTrackingPermission() {
        var d = new Date();
        d.setMonth(d.getMonth() + 24);
        document.cookie = "rlOk=1; expires=" + d.toUTCString() + "; path=/";
    }

    function onMessage(messageEvent) {
        userId = messageEvent.data["user_id"];
        replicateCookie(userId);
        return userId;
    }

    // from:
    // http://stackoverflow.com/questions/105034/how-to-create-a-guid-uuid-in-javascript
    function generateUuid() {
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
            var r = Math.random()*16|0, v = c == 'x' ? r : (r&0x3|0x8);
            return v.toString(16);
        });
    }

    function userId() {
        return loggedInUserId() || readCookie(rlId);
    }
    
    function loggedInUserId() {
    	return readCookie(rlId_loggedIn);
    }

    function userLogout() {
        deleteCookie(rlId_loggedIn);
    }

    function userLogin(userId, months) {
        return setCookie(rlId_loggedIn, userId, months);
    }

    return {
        init: function (endpoint, iframe, callback) {
            windowProxy = new Porthole.WindowProxy(endpoint + '/session/proxy', iframe);
            var user_id = userId();
            if (user_id) {
                callback(user_id);
            } else { // listen when there's no local cookie:
                windowProxy.addEventListener(function (e) {
                    var id = onMessage(e);
                    callback(id);
                });
            }
        },
        // The following should be factored into the above with an associated
        // reordering of arguments; for compatability, currently separate:
        ready: function (callback) {
            var id = userId();
            if (! id) {
                id = generateUuid();
                replicateCookie(id, 12); // lasts a year
            }
            callback(id);
        },
        blocked: privacyBlock,
        requestedPermission: requestedTrackingPermission,
        userId: userId,
        loggedInUserId: loggedInUserId,
        userLogin: userLogin,
        userLogout: userLogout
    };
})();
