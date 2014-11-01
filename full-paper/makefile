document.pdf: *.tex
	pdflatex document.tex
	bibtex document.aux
	pdflatex document.tex
	pdflatex document.tex

clean:
	rm *.aux *.bbl *.log *.blg document.pdf
