<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
    <!-- Replace the url with your own location -->
    <script type="text/javascript"
            src="<c:url value="/static/js/session/porthole.min.js"/>"></script>
    <script type="text/javascript">
        window.onload = function () {
            Porthole.WindowProxyDispatcher.start();
        };
    </script>
</head>
<body>
</body>
</html>
