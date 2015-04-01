<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8"/>
    <script type="text/javascript" src="<c:url value="/static/js/session/ender.min.js"/>"></script>
    <script type="text/javascript"
            src="<c:url value="/static/js/session/porthole.min.js"/>"></script>
    <script type="text/javascript">
        var windowProxy;

        function dispatchId(id) {
            windowProxy.post({ "user_id": id });
        }

         function dispatch() {
            var uuid = "${uuid}";
            dispatchId(uuid);
        }

        $.domReady(function () {
            windowProxy = new Porthole.WindowProxy('${proxy}');

            // For message receipt:
            windowProxy.addEventListener(function (event) {
            });

            if ( $.browser.msie  && $.browser.version <= 8) {
                // Workaround: delay before dispatch.
                var ieDelay = 150;
                setTimeout(dispatch, ieDelay);
            } else {
                dispatch();
            }

            <c:if test="${closeImmediately}">
                window.close();
            </c:if>
        });
    </script>
</head>
<body>
</body>
</html>
