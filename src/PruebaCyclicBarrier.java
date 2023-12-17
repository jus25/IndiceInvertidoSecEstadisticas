import java.util.Random;
import java.util.concurrent.CyclicBarrier;

public class PruebaCyclicBarrier {
    public static void main(String[] args) {
        int numeroHilos = 5;

        CyclicBarrier[] barreras = new CyclicBarrier[2];
        barreras[0] = new CyclicBarrier(numeroHilos + 1);
        barreras[1] = new CyclicBarrier(numeroHilos + 1);


        PruebaHilo p = new PruebaHilo(barreras);
        for (int i = 0; i < numeroHilos; i++) {
            Thread hilo = new Thread(p);
            hilo.start();
        }

        try {
            System.out.println("levanto barrera");
            barreras[0].await();
            barreras[1].await();
            System.out.println("todo acabado");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    private static class PruebaHilo implements Runnable {
        CyclicBarrier barreraInicio;
        CyclicBarrier barreraFin;

        public PruebaHilo(CyclicBarrier[] barreras){
            this.barreraInicio=barreras[0];
            this.barreraFin=barreras[1];
        }

        @Override
        public void run() {
            try {
                barreraInicio.await();
                long secs = new Random().nextInt(1,10);
                System.out.println("hilo ejecutandose (Carga de trabajo de "+secs+"s)");
                Thread.sleep(secs*1000); //Imita el tiempo que tarda en realizar el trabajo

                barreraFin.await();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
