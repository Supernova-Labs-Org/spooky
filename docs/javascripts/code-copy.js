(function () {
  "use strict";

  function addCopyButtons() {
    var blocks = document.querySelectorAll("article .typography pre");
    blocks.forEach(function (pre) {
      if (pre.dataset.copyBound === "1") {
        return;
      }

      var code = pre.querySelector("code");
      if (!code) {
        return;
      }

      pre.dataset.copyBound = "1";

      var button = document.createElement("button");
      button.type = "button";
      button.className = "code-copy-button";
      button.textContent = "Copy";
      button.setAttribute("aria-label", "Copy code to clipboard");

      button.addEventListener("click", function () {
        var text = code.innerText.replace(/\n$/, "");
        navigator.clipboard
          .writeText(text)
          .then(function () {
            button.textContent = "Copied";
            pre.classList.add("is-copied");
            setTimeout(function () {
              button.textContent = "Copy";
              pre.classList.remove("is-copied");
            }, 1200);
          })
          .catch(function () {
            button.textContent = "Failed";
            setTimeout(function () {
              button.textContent = "Copy";
            }, 1200);
          });
      });

      pre.appendChild(button);
    });
  }

  document.addEventListener("DOMContentLoaded", function () {
    addCopyButtons();

    var observer = new MutationObserver(function () {
      addCopyButtons();
    });

    observer.observe(document.body, {
      childList: true,
      subtree: true,
    });
  });
})();
