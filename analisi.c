
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <pthread.h>

#define MAXCHAR 500

#define LEN_CODE_AIRPORT 3
#define STR_CODE_AIRPORT (LEN_CODE_AIRPORT+1) // Incluimos el caracter de final de palabra '\0'
#define NUM_AIRPORTS 303

#define COL_ORIGIN_AIRPORT 17
#define COL_DESTINATION_AIRPORT 18

#define F 2
#define B 10
#define N 100

pthread_mutex_t main_mutex=PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex2=PTHREAD_MUTEX_INITIALIZER;

pthread_cond_t consumer = PTHREAD_COND_INITIALIZER; 
pthread_cond_t producer = PTHREAD_COND_INITIALIZER;

typedef struct cell 
{
    int nelems;
    char **lines;
    int cellToRead;
} cell;
typedef struct buffer 
{
    cell **cells;
} buffer;
typedef struct arguments{
    buffer buffer;
    int **num_flights;
    char **airports;
    
}arguments;
int size = 0;
/**
 * Reserva espacio para una matriz de tamaño nrow x ncol,
 * donde cada elemento de la matriz tiene size bytes
 */

void **calloc_matrix(int nrow, int ncol, size_t size)
{
  int i;

  void **ptr;

  ptr = calloc(size, sizeof(void *) * nrow);
  for(i = 0; i < nrow; i++)
    ptr[i] = calloc(size, sizeof(size) * ncol);

  return ptr;
}

/**
 * Libera una matriz de tamaño con nrow filas. Utilizar
 * la funcion malloc_matrix para reservar la memoria 
 * asociada.
 */

void free_matrix(void **matrix, int nrow)
{
  int i;

  for(i = 0; i < nrow; i++)
    free(matrix[i]);
}

/**
 * Leer el fichero fname que contiene los codigos
 * IATA (3 caracteres) de los aeropuertos a analizar.
 * En total hay NUM_AIRPORTS a leer, un valor prefijado
 * (para simplificar el código). Los codigos de los
 * aeropuertos se alacenan en la matriz airports, una
 * matriz cuya memoria ha sido previamente reservada.
 */

void read_airports(char **airports, char *fname) 
{
  int i;
  char line[MAXCHAR];

  FILE *fp;

  /*
   * eow es el caracter de fin de palabra
   */
  char eow = '\0';

  fp = fopen(fname, "r");
  if (!fp) {
    printf("ERROR: could not open file '%s'\n", fname);
    exit(1);
  }

  i = 0;
  while (i < NUM_AIRPORTS)
  {
    fgets(line, 100, fp);
    line[3] = eow; 

    /* Copiamos los datos al vector */
    strcpy(airports[i], line);

    i++;
  }

  fclose(fp);
}

/**
 * Dada la matriz de con los codigos de los aeropuertos,
 * así como un código de aeropuerto, esta función retorna
 * la fila asociada al aeropuerto.
 */

int get_index_airport(char *code, char **airports)
{
  int i;

  for(i = 0; i < NUM_AIRPORTS; i++) 
    if (strcmp(code, airports[i]) == 0)
      return i;

  return -1;
}


/**
 * Dada la matriz num_flights, se imprimen por pantalla el
 * numero de destinos diferentes que tiene cada aeropuerto.
 */

void print_num_flights_summary(int **num_flights, char **airports)
{
  int i, j, num;

  for(i = 0; i < NUM_AIRPORTS; i++) 
  {
    num = 0;

    for(j = 0; j < NUM_AIRPORTS; j++)
    {
      if (num_flights[i][j] > 0)
        num++;
    }

    printf("Origin: %s -- Number of different destinations: %d\n", airports[i], num);
  }
}

/**
 * Esta funcion se utiliza para extraer informacion del fichero CSV que
 * contiene informacion sobre los vuelos. En particular, dada una linea
 * leida de fichero, la funcion extra el origen y destino de los vuelos.
 */

int extract_fields_airport(char *origin, char *destination, char *line) 
{
  /*Recorre la linea por caracteres*/
  char caracter;
  /* i sirve para recorrer la linea
   * iterator es para copiar el substring de la linea a char
   * coma_count es el contador de comas
   */
  int i, iterator, coma_count;
  /* start indica donde empieza el substring a copiar
   * end indica donde termina el substring a copiar
   * len indica la longitud del substring
   */
  int start, end, len;
  /* invalid nos permite saber si todos los campos son correctos
   * 1 hay error, 0 no hay error 
   */
  int invalid = 0;
  /* found se utiliza para saber si hemos encontrado los dos campos:
   * origen y destino
   */
  int found = 0;
  /*
   * eow es el caracter de fin de palabra
   */
  char eow = '\0';
  /*
   * contenedor del substring a copiar
   */
  char word[STR_CODE_AIRPORT];
  /*
   * Inicializamos los valores de las variables
   */
  start = 0;
  end = -1;
  i = 0;
  coma_count = 0;
  /*
   * Empezamos a contar comas
   */
  do {
    caracter = line[i++];
    if (caracter == ',') {
      coma_count ++;
      /*
       * Cogemos el valor de end
       */
      end = i;
      /*
       * Si es uno de los campos que queremos procedemos a copiar el substring
       */
      if (coma_count ==  COL_ORIGIN_AIRPORT || coma_count == COL_DESTINATION_AIRPORT) {
        /*
         * Calculamos la longitud, si es mayor que 1 es que tenemos 
         * algo que copiar
         */
        len = end - start;

        if (len > 1) {

          if (len > STR_CODE_AIRPORT) {
            printf("ERROR len code airport\n");
            exit(1);
          }

          /*
           * Copiamos el substring
           */
          for(iterator = start; iterator < end-1; iterator ++){
            word[iterator-start] = line[iterator];
          }
          /*
           * Introducimos el caracter de fin de palabra
           */
          word[iterator-start] = eow;
          /*
           * Comprobamos que el campo no sea NA (Not Available) 
           */
          if (strcmp("NA", word) == 0)
            invalid = 1;
          else {
            switch (coma_count) {
              case COL_ORIGIN_AIRPORT:
                strcpy(origin, word);
                found++;
                break;
              case COL_DESTINATION_AIRPORT:
                strcpy(destination, word);
                found++;
                break;
              default:
                printf("ERROR in coma_count\n");
                exit(1);
            }
          }

        } else {
          /*
           * Si el campo esta vacio invalidamos la linea entera 
           */

          invalid = 1;
        }
      }
      start = end;
    }
  } while (caracter && invalid==0);

  if (found != 2)
    invalid = 1;

  return invalid;
}

void *thread_consumidor(void *arg){
    int i,toReadIndex;
    char origin[STR_CODE_AIRPORT], destination[STR_CODE_AIRPORT];
    int invalid, index_origin, index_destination;
    arguments *arguments;
    buffer buffer;
    int **num_flights;
    char **airports;
    cell *cell_consumer, *cell_buffer, *tmp;
    /*reservamos memoria para la celda*/
    cell_consumer = calloc(1,sizeof(cell));
    cell_consumer->nelems = 0;
    cell_consumer->cellToRead = 0;
    cell_consumer->lines = calloc(N,sizeof(char*));
    for(i = 0;i < N;i++){
        cell_consumer->lines[i] = calloc(MAXCHAR,sizeof(char));
    }
    while(1){
    	/*seccion critica*/
        pthread_mutex_lock(&main_mutex);
        /*argumentos de hilo*/
        arguments = arg;
        num_flights=arguments->num_flights;
        airports=arguments->airports;
        buffer=arguments->buffer;
        /*si el buffer esta vacia, esperamos la señal que indica que ya no*/
        while(size == 0){
            pthread_cond_wait(&consumer,&main_mutex);
        }
        toReadIndex = -1;
        /*buscamos una celda no vacia para extraer datos*/
        for(i = 0;i < B;i++){
            if(buffer.cells[i]->cellToRead == 1){
                toReadIndex = i;
                break;
            }
        }

      
        //intercambio de celdas
        cell_buffer = buffer.cells[toReadIndex];
        tmp = buffer.cells[toReadIndex];
        buffer.cells[toReadIndex] = cell_consumer;
        cell_consumer = tmp;
        size--;
        cell_consumer->cellToRead = 0;
        //desbloqueamos el productor
        pthread_cond_signal(&producer);
        
        pthread_mutex_unlock(&main_mutex);
        /*Si se detecta una celda para leer que tiene 0 nelems el productor
          nos esta indicando que debemos salir del hilo
         */
        if(tmp->nelems == 0){
            break;
        }
        
        for(i = 0; i < cell_consumer->nelems; i++) {

	    invalid = extract_fields_airport(origin, destination, cell_consumer->lines[i]);

	    if (!invalid) {
	      index_origin = get_index_airport(origin, airports);
	      index_destination = get_index_airport(destination, airports);

	      if ((index_origin >= 0) && (index_destination >= 0) 
              && (index_destination < NUM_AIRPORTS) && (index_origin < NUM_AIRPORTS)){
	      
	      pthread_mutex_lock(&mutex2);
	      num_flights[index_origin][index_destination]++;
	      pthread_mutex_unlock(&mutex2);
          }
	    }
    }    
    }
    /*liberamos memoria*/
    for(i = 0; i < N; i++)
        free(cell_consumer->lines[i]);
    free(cell_consumer->lines);
    free(cell_consumer);
    
    return ((void *) 0);
}
/**
 * Dado un fichero CSV que contiene informacion entre multiples aeropuertos,
 * esta funcion lee cada linea del fichero y actualiza la matriz num_flights
 * para saber cuantos vuelos hay entre cada cuidad origen y destino.
 */ 

void read_airports_data(int **num_flights, char **airports, char *fname) 
{
    int i,j,linesCount,nullElemsCount,ended;
    char line[MAXCHAR];
    pthread_t ntid[F];

    FILE *fp;

    fp = fopen(fname, "r");
    if (!fp) {
    printf("ERROR: could not open '%s'\n", fname);
    exit(1);
    }

    /* Leemos la cabecera del fichero */
    fgets(line, MAXCHAR, fp);
    
    /*Creamos el buffer y reservamos memoria para él*/
    buffer buffer;
    buffer.cells = calloc(B,sizeof(cell*));
    
    for(i = 0;i<B;i++){
        * (buffer.cells + i) = calloc(1,sizeof(cell));
        buffer.cells[i]->nelems = 0;
        buffer.cells[i]->cellToRead = 0;
        buffer.cells[i]->lines = calloc(N,sizeof(char*));
        for(j = 0;j < N;j++){
            buffer.cells[i]->lines[j] = calloc(MAXCHAR,sizeof(char));
        }
    }
    /*preparamos el argumento para los hilos*/
    arguments arguments;
    arguments.buffer = buffer;
    arguments.airports = airports;
    arguments.num_flights = num_flights;
    /*creamos los consumidores*/
    for(i = 0;i < F;i++){
        pthread_create(&(ntid[i]),NULL, thread_consumidor, (void *) &arguments);
    }
    cell *cell_producer, *cell_buffer, *tmp;

    /*reservamos memoria para cell_producer*/
    cell_producer = calloc(1,sizeof(cell));
    cell_producer->nelems = 0;
    cell_producer->cellToRead = 1;
    cell_producer->lines = calloc(N,sizeof(char*));
    for(i = 0;i < N;i++){
        cell_producer->lines[i] = calloc(MAXCHAR,sizeof(char));
    }
    nullElemsCount = 0;
    ended = 0;
    /*acaba cuando acabamos el fichero, pero antes llenamos el buffer con F celdas 
     * con nelems = 0, para indicar a los consumidores que deben acabar la ejecución
     */
    while(nullElemsCount < F){
        
        linesCount = 0;
        if(ended == 0){
            for(i = 0; i < N; i++) {
                /*Obtenemos las linies*/
                if(fgets(cell_producer->lines[i], MAXCHAR, fp) == NULL){
                    ended = 1;
                    break;
                }
                linesCount++;
            }
        }
        /*para evitar el caso de que el productor lee hasta al final del fichero
        sin llegar N linies
        */
        cell_producer->nelems = linesCount; 
        /*cuando ha leido todo el fichero, nullelemscount aumenta*/
        if(linesCount == 0){
            nullElemsCount++;
        }
        /*Sección crítica*/
        pthread_mutex_lock(&main_mutex);
        /*mientras que el buffer esta lleno, esperamos hasta que
        un consumidor envia una señal indicando que ya no esta lleno.
        */
        while(size >= B){
            pthread_cond_wait(&producer,&main_mutex);
        }
        
        /*Buscamos una celda vacia*/
        int emptyIndex = -1;
        for(i = 0; i < B;i++){
            if(buffer.cells[i]->cellToRead == 0){
                emptyIndex = i;
                break;
            }
        }
        /*intercambio de celdas*/ 
        cell_producer->cellToRead = 1;
        tmp = buffer.cells[emptyIndex];
        buffer.cells[emptyIndex] = cell_producer;
        cell_producer = tmp;
        size++;
        
	/*desbloqueamos un consumidor*/
        pthread_cond_broadcast(&consumer);
        pthread_mutex_unlock(&main_mutex);
        
    }
    /*Esperamos que acabe el hilo productor*/
    for(i=0; i<F; i++){
      pthread_join(ntid[i],NULL);
    }
    /*liberamos la memoria*/
    for(i = 0; i < N; i++)
        free(cell_producer->lines[i]);
    free(cell_producer->lines);
    free(cell_producer);
    for(i = 0; i < B; i++){
    for(j = 0; j < N; j++){
        free(buffer.cells[i]->lines[j]);
    }
    free(buffer.cells[i]->lines);
    free(buffer.cells[i]);	
    }
    free(buffer.cells);
    fclose(fp);
}

/**
 * Esta es la funcion principal que realiza los siguientes procedimientos
 * a) Lee los codigos IATA del fichero de aeropuertos
 * b) Lee la informacion de los vuelos entre diferentes aeropuertos y
 *    actualiza la matriz num_flights correspondiente.
 * c) Se imprime para cada aeropuerto origen cuantos destinos diferentes
 *    hay.
 * d) Se imprime por pantalla lo que ha tardado el programa para realizar
 *    todas estas tareas.
 */

int main(int argc, char **argv)
{
  char **airports;
  int **num_flights;

  if (argc != 3) {
    printf("%s <airport.csv> <flights.csv>\n", argv[0]);
    exit(1);
  }

  struct timeval tv1, tv2;

  // Tiempo cronologico 
  gettimeofday(&tv1, NULL);

  // Reserva espacio para las matrices
  airports    = (char **) calloc_matrix(NUM_AIRPORTS, STR_CODE_AIRPORT, sizeof(char));
  num_flights = (int **) calloc_matrix(NUM_AIRPORTS, NUM_AIRPORTS, sizeof(int));

  // Lee los codigos de los aeropuertos 
  read_airports(airports, argv[1]);

  // Lee los datos de los vuelos
  read_airports_data(num_flights, airports, argv[2]);

  // Imprime un resumen de la tabla
  print_num_flights_summary(num_flights, airports);

  // Libera espacio
  free_matrix((void **) airports, NUM_AIRPORTS);
  free_matrix((void **) num_flights, NUM_AIRPORTS);
  free(airports);
  free(num_flights);
  // Tiempo cronologico 
  gettimeofday(&tv2, NULL);

  // Tiempo para la creacion del arbol 
  printf("Tiempo para procesar el fichero: %f segundos\n",
      (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
      (double) (tv2.tv_sec - tv1.tv_sec));

  return 0;
}
