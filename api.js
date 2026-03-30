<html>
<body>
    <script src="https://js.puter.com/v2/"></script>
    <script>
        puter.ai.chat(
            prompt,
            { model: "gpt-4.1" }
        ).then(response => {
            puter.print("<h2>Using gpt-4.1 model</h2>");
            puter.print(response);
        });
    </script>
</body>
</html>