package org.janelia.stitching;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.commons.io.IOUtils;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import ij.ImagePlus;
import mpicbg.stitching.ImageCollectionElement;
import mpicbg.stitching.StitchingParameters;

/**
 * @author pisarevi
 *
 */

public class StitchingJob implements Serializable {
	
	private static final long serialVersionUID = 2619120742300093982L;
	
	private StitchingArguments args;
	private transient StitchingParameters params;
	private String baseImagesFolder; 
	private TileInfo[] tiles;
	private int dimensionality;
	
	public StitchingJob( final StitchingArguments args ) {
		this.args = args;
		baseImagesFolder = new File( args.getInput() ).getAbsoluteFile().getParent();
	}
	
	protected StitchingJob( ) {	}
	
	public StitchingParameters getParams() {
		return params;
	}
	
	public void setParams( final StitchingParameters params ) {
		this.params = params;
	}
	
	public TileInfo[] getTiles() {
		return tiles;
	}
	
	public String getBaseImagesFolder() {
		return baseImagesFolder;
	}
	
	public int getDimensionality() {
		return dimensionality;
	}
	
	public void prepareTiles() throws Exception {
		loadTiles();
		setUpTiles();
	}
	
	private void loadTiles() throws FileNotFoundException {
		final JsonReader reader = new JsonReader( new FileReader( args.getInput() ) );
		tiles = new Gson().fromJson( reader, TileInfo[].class );
		
		boolean malformed = ( tiles == null );
		if ( !malformed )
			for ( final TileInfo tile : tiles )
				if ( tile == null )
					malformed = true;
		
		if ( malformed )
			throw new NullPointerException( "Malformed input JSON file" );
	}
	
	public void saveTiles( final TileInfo[] resultingTiles ) throws IOException {
		final StringBuilder output = new StringBuilder( args.getInput() );
		int lastDotIndex = output.lastIndexOf( "." );
		if ( lastDotIndex == -1 )
			lastDotIndex = output.length();
		output.insert( lastDotIndex, "_output" );
		
		final FileWriter writer = new FileWriter( output.toString() );
		writer.write( new Gson().toJson( resultingTiles ) );
		writer.close();
	}
	
	private void setUpTiles() throws Exception {
		
		for ( int i = 0; i < tiles.length; i++ ) {
			if ( tiles[ i ].getFile() == null || tiles[ i ].getPosition() == null )
				throw new NullPointerException( "Some of required parameters are missing (file or position)" );
			
			if ( tiles[ i ].getIndex() == null )
				tiles[ i ].setIndex( i );
		}
	}
	
	public void validateTiles() throws IllegalArgumentException {
		if ( tiles.length < 2 )
			throw new IllegalArgumentException( "There must be at least 2 tiles in the dataset" );
		
		for ( int i = 0; i < tiles.length; i++ )
			if ( tiles[ i ].getPosition().length != tiles[ i ].getSize().length )
				throw new IllegalArgumentException( "Incorrect dimensionality" );
		
		for ( int i = 1; i < tiles.length; i++ )
			if ( tiles[ i ].getDimensionality() != tiles[ i - 1 ].getDimensionality() )
				throw new IllegalArgumentException( "Incorrect dimensionality" );
		
		// Everything is correct
		this.dimensionality = tiles[ 0 ].getDimensionality();
	}
	
	
	// TODO: pull request for making StitchingParameters serializable, then remove it
	private void writeObject( final ObjectOutputStream stream ) throws IOException {
		stream.defaultWriteObject();
		stream.write( new Gson().toJson( params ).getBytes() );
	}
	private void readObject( final ObjectInputStream stream ) throws IOException, ClassNotFoundException {
		stream.defaultReadObject();
		params = new Gson().fromJson( IOUtils.toString(stream), StitchingParameters.class );
	}
}
