package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	dataframe "github.com/kfultz07/go-dataframe"
)

func ReadMoviesCsvFile(filename string) dataframe.DataFrame {
	/*file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1 // Permite un número variable de campos por registro

	// Leer todas las filas del archivo
	rows, err := reader.Read()
	if err != nil {
		return nil, err
	}

	var movies []Movie
	for i, row := range rows {
		if i == 0 {
			// Salta la primera fila si es el encabezado
			continue
		}
		movies = append(movies, Movie{
			MovieID: row[0],
			Title:   row[1],
			Genres:  row[2],
		})
	}

	return movies */
	//path := "movies.csv"
	path := "../FileFrag/dat/in/"

	df := dataframe.CreateDataFrame(path, filename)

	return df
}

func ReadRatingsCsvFile(filename string) dataframe.DataFrame {
	/*file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1 // Permite un número variable de campos por registro

	// Leer todas las filas del archivo
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	var ratings []Rating
	for i, row := range rows {
		if i == 0 {
			// Salta la primera fila si es el encabezado
			continue
		}
		rating, err := strconv.ParseFloat(row[2], 64)
		if err != nil {
			return nil, err
		}
		ratings = append(ratings, Rating{
			UserID:  row[0],
			MovieID: row[1],
			Rating:  rating,
		})
	}

	return ratings, nil */
	path := "../FileFrag/dat/out/"

	df := dataframe.CreateDataFrame(path, filename)

	return df
}

func Mt_FindRatingsMaster() {
	fmt.Println("In MtFindRatingsMaster")
	start := time.Now()
	nf := 7 // number of files with ratings is also a number of threads for multi-threading

	// kg is a 1D array that contains the Known Genres
	kg := []string{"Action", "Adventure", "Animation", "Children", "Comedy", "Crime", "Documentary",
		"Drama", "Fantasy", "Film-Noir", "Horror", "IMAX", "Musical", "Mystery", "Romance",
		"Sci-Fi", "Thriller", "War", "Western", "(no genres listed)"}

	ng := len(kg) //number of known genres
	/* ra is a 2D array where the ratings values for each genre are maintained.
	The columns signal/maintain the core number where a worker is running
	The rows in that column maintain the raiting values for that core and that genre */
	ra := make([][]float64, ng)
	/* ca is a 2D array where the count of Ratings for each genre are maintained.
	The columns signal the core number where the worker is running
	The rows in that column maintain the counts for that genre */
	ca := make([][]int, ng)
	// populate the ng rows of ra and ca with nf columns
	for i := 0; i < ng; i++ {
		ra[i] = make([]float64, nf)
		ca[i] = make([]int, nf)
	}
	var ci = make(chan int) // create the channel to sync all workers
	movies := ReadMoviesCsvFile("movies.csv")
	// run FindRatings in 10 workers
	for i := 0; i < nf; i++ {
		go Mt_FindRatingsWorker(i+1, ci, kg, &ca, &ra, movies)
	}

	//wait for the workers
	iMsg := 0
	go func() {
		for {
			i := <-ci
			iMsg += i
		}
	}()
	for {
		if iMsg == nf {
			break
		}
	}
	// all workers completed their work. Collect results and produce report
	locCount := make([]int, ng)
	locVals := make([]float64, ng)
	for i := 0; i < ng; i++ {
		for j := 0; j < nf; j++ {
			locCount[i] += ca[i][j]
			locVals[i] += ra[i][j]
		}
	}
	for i := 0; i < ng; i++ {
		fmt.Println(fmt.Sprintf("%2d", i), " ", fmt.Sprintf("%20s", kg[i]), " ", fmt.Sprintf("%8d", locCount[i]))
	}
	duration := time.Since(start)
	fmt.Println("Duration = ", duration)
	println("Mt_FindRatingsMaster is Done")
}

func Mt_FindRatingsWorker(w int, ci chan int, kg []string, ca *[][]int, va *[][]float64, movies dataframe.DataFrame) {
	aFileName := "ratings_" + fmt.Sprintf("%d", w-1) + ".csv"
	println("Worker  ", fmt.Sprintf("%02d", w-1), " is processing file ", aFileName, "\n")

	ratings := ReadRatingsCsvFile(aFileName)
	ng := len(kg)
	start := time.Now()

	//import all records from the movies DF into the ratings DF, keeping genres column from movies
	// df.Merge is the equivalent of an inner-join in the DF lib I am using here
	ratings.Merge(&movies, "movieId", "genres")

	// We only need "genres" and "ratings" to find Count(Ratings | Generes), so keep only those columns
	grcs := [2]string{"genres", "rating"} // grcs => Genres Ratings Columns
	grDF := ratings.KeepColumns(grcs[:])  // grDF => Genres Ratings DF
	for ig := 0; ig < ng; ig++ {
		for _, row := range grDF.FrameRecords {
			if strings.Contains(row.Data[0], kg[ig]) {
				(*ca)[ig][w-1] += 1
				v, _ := strconv.ParseFloat((row.Data[1]), 32) // do not check for error
				(*va)[ig][w-1] += v
			}
		}
	}
	duration := time.Since(start)
	fmt.Println("Duration = ", duration)
	fmt.Println("Worker ", w, " completed")

	// notify master that this worker has completed its job

	ci <- 1
}

func main() {
	Mt_FindRatingsMaster()
}
