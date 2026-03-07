;; -*- lexical-binding: t; -*-

(TeX-add-style-hook
 "DM_Project"
 (lambda ()
   (setq TeX-command-extra-options
         "--shell-escape")
   (TeX-add-to-alist 'LaTeX-provided-class-options
                     '(("article" "a4paper" "10pt")))
   (TeX-add-to-alist 'LaTeX-provided-package-options
                     '(("AtkinsonHyperlegible" "sfdefault") ("graphicx" "") ("atkinson" "sfdefault") ("textcomp" "") ("enumitem" "") ("amsmath" "") ("geometry" "a4paper" "total={7in, 10in}") ("titlesec" "") ("fancyhdr" "") ("fontenc" "T1") ("hyperref" "") ("svg" "") ("adjustbox" "export") ("contour" "") ("ulem" "")))
   (add-to-list 'LaTeX-verbatim-macros-with-braces-local "href")
   (add-to-list 'LaTeX-verbatim-macros-with-braces-local "hyperimage")
   (add-to-list 'LaTeX-verbatim-macros-with-braces-local "hyperbaseurl")
   (add-to-list 'LaTeX-verbatim-macros-with-braces-local "nolinkurl")
   (add-to-list 'LaTeX-verbatim-macros-with-braces-local "url")
   (add-to-list 'LaTeX-verbatim-macros-with-braces-local "path")
   (add-to-list 'LaTeX-verbatim-macros-with-delims-local "path")
   (TeX-run-style-hooks
    "latex2e"
    "article"
    "art10"
    "textcomp"
    "enumitem"
    "amsmath"
    "geometry"
    "titlesec"
    "fancyhdr"
    "fontenc"
    "hyperref"
    "svg"
    "adjustbox"
    "contour"
    "ulem")
   (TeX-add-symbols
    '("myuline" 1))
   (LaTeX-add-labels
    "fig:IMDb Relational Schema"
    "fig:Excerpt of RDF Graph."))
 :latex)

