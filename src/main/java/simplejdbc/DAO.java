package simplejdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.naming.spi.DirStateFactory;
import javax.sql.DataSource;

public class DAO {

	private final DataSource myDataSource;

	/**
	 *
	 * @param dataSource la source de donnÃ©es Ã  utiliser
	 */
	public DAO(DataSource dataSource) {
		this.myDataSource = dataSource;
	}

	/**
	 *
	 * @return le nombre d'enregistrements dans la table CUSTOMER
	 * @throws DAOException
	 */
	public int numberOfCustomers() throws DAOException {
		int result = 0;

		String sql = "SELECT COUNT(*) AS NUMBER FROM CUSTOMER";
		// Syntaxe "try with resources" 
		// cf. https://stackoverflow.com/questions/22671697/try-try-with-resources-and-connection-statement-and-resultset-closing
		try (Connection connection = myDataSource.getConnection(); // Ouvrir une connexion
			Statement stmt = connection.createStatement(); // On crÃ©e un statement pour exÃ©cuter une requÃªte
			ResultSet rs = stmt.executeQuery(sql) // Un ResultSet pour parcourir les enregistrements du rÃ©sultat
			) {
			if (rs.next()) { // Pas la peine de faire while, il y a 1 seul enregistrement
				// On rÃ©cupÃ¨re le champ NUMBER de l'enregistrement courant
				result = rs.getInt("NUMBER");
			}
		} catch (SQLException ex) {
			Logger.getLogger("DAO").log(Level.SEVERE, null, ex);
			throw new DAOException(ex.getMessage());
		}

		return result;
	}
	
	/**
	 * Detruire un enregistrement dans la table CUSTOMER
	 * @param customerId la clÃ© du client Ã  dÃ©truire
	 * @return le nombre d'enregistrements dÃ©truits (1 ou 0 si pas trouvÃ©)
	 * @throws DAOException
	 */
	public int deleteCustomer(int customerId) throws DAOException {

		// Une requÃªte SQL paramÃ©trÃ©e
		String sql = "DELETE FROM CUSTOMER WHERE CUSTOMER_ID = ?";
		try (   Connection connection = myDataSource.getConnection();
			PreparedStatement stmt = connection.prepareStatement(sql)
                ) {
                        // DÃ©finir la valeur du paramÃ¨tre
			stmt.setInt(1, customerId);
			
			return stmt.executeUpdate();

		}  catch (SQLException ex) {
			Logger.getLogger("DAO").log(Level.SEVERE, null, ex);
			throw new DAOException(ex.getMessage());
		}
	}	

	/**
	 *
	 * @param customerId la clÃ© du client Ã  recherche
	 * @return le nombre de bons de commande pour ce client (table PURCHASE_ORDER)
	 * @throws DAOException
	 */
	public int numberOfOrdersForCustomer(int customerId) throws DAOException {
				int result = 0;

		String sql = "SELECT COUNT(*) AS NUMBER FROM PURCHASE_ORDER WHERE CUSTOMER_ID = ?";
		// Syntaxe "try with resources" 
		// cf. https://stackoverflow.com/questions/22671697/try-try-with-resources-and-connection-statement-and-resultset-closing
		try (Connection connection = myDataSource.getConnection(); // Ouvrir une connexion
			PreparedStatement stmt = connection.prepareStatement(sql)
			) {
                        stmt.setInt(1, customerId);
                        
                        ResultSet rs = stmt.executeQuery() ;
                        
			if (rs.next()) { // Pas la peine de faire while, il y a 1 seul enregistrement
				// On rÃ©cupÃ¨re le champ NUMBER de l'enregistrement courant
				result = rs.getInt("NUMBER");
                                
                             
			}
		} catch (SQLException ex) {
			Logger.getLogger("DAO").log(Level.SEVERE, null, ex);
			throw new DAOException(ex.getMessage());
		}

		return result;
	}

	/**
	 * Trouver un Customer Ã  partir de sa clÃ©
	 *
	 * @param customerID la clÃ© du CUSTOMER Ã  rechercher
	 * @return l'enregistrement correspondant dans la table CUSTOMER, ou null si pas trouvÃ©
	 * @throws DAOException
	 */
	CustomerEntity findCustomer(int customerID) throws DAOException {
                CustomerEntity c = null;
		String sql = "SELECT * FROM CUSTOMER WHERE CUSTOMER_ID = ?";
		// Syntaxe "try with resources" 
		// cf. https://stackoverflow.com/questions/22671697/try-try-with-resources-and-connection-statement-and-resultset-closing
		try (Connection connection = myDataSource.getConnection(); // Ouvrir une connexion
			PreparedStatement stmt = connection.prepareStatement(sql)
			) {
                        stmt.setInt(1, customerID);
                        
                        ResultSet rs = stmt.executeQuery() ;

			if (rs.next()) { // Pas la peine de faire while, il y a 1 seul enregistrement
				// On rÃ©cupÃ¨re le champ NUMBER de l'enregistrement courant
                             
                             c = new CustomerEntity(rs.getInt("CUSTOMER_ID"), rs.getString("NAME"), rs.getString("ADDRESSLINE1"));
                            
                            
                        }
		} catch (SQLException ex) {
			Logger.getLogger("DAO").log(Level.SEVERE, null, ex);
			throw new DAOException(ex.getMessage());
		}

		return c;
	}

	/**
	 * Liste des clients localisÃ©s dans un Ã©tat des USA
	 *
	 * @param state l'Ã©tat Ã  rechercher (2 caractÃ¨res)
	 * @return la liste des clients habitant dans cet Ã©tat
	 * @throws DAOException
	 */
	List<CustomerEntity> customersInState(String state) throws DAOException {
		List<CustomerEntity> lcust = new LinkedList<>();
		String sql = "SELECT * FROM CUSTOMER WHERE STATE = ?";
		// Syntaxe "try with resources" 
		
		try (Connection connection = myDataSource.getConnection(); // Ouvrir une connexion
			PreparedStatement stmt = connection.prepareStatement(sql)
			) {
                        stmt.setString(1, state);
                        
                        ResultSet rs = stmt.executeQuery() ;

			while(rs.next()) { // Pas la peine de faire while, il y a 1 seul enregistrement
                            CustomerEntity c = new CustomerEntity(rs.getInt("CUSTOMER_ID"), rs.getString("NAME"), rs.getString("ADDRESSLINE1"));
                    // On rÃ©cupÃ¨re le champ NUMBER de l'enregistrement courant
                            lcust.add(c);
                        }
		} catch (SQLException ex) {
			Logger.getLogger("DAO").log(Level.SEVERE, null, ex);
			throw new DAOException(ex.getMessage());
		}
              
		return lcust;
	}

}
